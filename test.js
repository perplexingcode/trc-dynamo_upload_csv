const AWS = require('aws-sdk');
const fs = require('fs');
const batchSize = 25;
const concurrentRequests = 40;
const readline = require('readline');
const { REGION } = require('./CONFIG');
const { v4: uuidv4 } = require('uuid');
const date = new Date();
const today = `${
  date.getMonth() + 1 < 10 ? '0' + (date.getMonth() + 1) : date.getMonth()
}${date.getDate() < 10 ? '0' + date.getDate() : date.getDate()}`;

TABLE_NAME = 'thaybo';

const batchUploadDDB = function (table, items) {
  AWS.config.update({
    region: REGION,
  });
  console.log(`   Upload started.
`);
  const dynamodb = new AWS.DynamoDB.DocumentClient();
  var start = new Date().getTime();

  async function saveToDynamoDB(items) {
    const putReqs = items.map((item) => ({
      PutRequest: {
        Item: item,
      },
    }));

    const req = {
      RequestItems: {
        [`${TABLE_NAME}`]: putReqs,
      },
    };

    await dynamodb.batchWrite(req).promise();
  }

  (async function () {
    let batchNo = 0;
    let promises = [];

    if (items.length % batchSize === 0) {
      console.log(`   Batch: ${batchNo}`);
      promises.push(saveToDynamoDB(items));
      if (promises.length % concurrentRequests === 0) {
        console.log('\n   ...Awaiting write requests to DynamoDB...\n');
        console.log(`   ${batchNo * batchSize} items uploaded.`);
        console.log(
          `   ${parseInt(timep / 1000)} seconds elapsed. (${parseInt(
            timep / 60000
          )} minutes)`
        );
        await Promise.all(promises);
        promises = [];
        var endp = new Date().getTime();
        var timep = endp - start;
      }

      items = [];
      batchNo++;
    }

    if (items.length > 0) {
      console.log(`   Batch: ${batchNo}`);
      promises.push(saveToDynamoDB(items));
    }

    if (promises.length > 0) {
      console.log('\n   ...Finishing up...\n');
      await Promise.all(promises);
    }
    var end = new Date().getTime();
    var time = end - start;
    const message = `---------------------------
    ${today}
    Operation completed!
    ${batchNo} packages ~ ${
      batchNo * batchSize
    } items uploaded to ${TABLE_NAME} table.
    Total time: ${parseInt(time / 60000)} minutes`;
    console.log(message);
  })();
};
