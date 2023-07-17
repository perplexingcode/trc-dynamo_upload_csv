const AWS = require("aws-sdk");
const fs = require("fs");
const csvWriter = require("csv-write-stream");
const batchSize = 20;
const concurrentRequests = 10;
process.env.AWS_NODEJS_CONNECTION_REUSE_ENABLED = 1;
const readline = require("readline");
const {
  REGION,
  FILE_DIR,
  TABLE_NAME,
  IGNORE_FIRST_ROW,
  CHECK_DATA,
  convertToObject,
  columns,
  CHECK_MISSING,
  ITEM_ID,
} = require("./CONFIG");
const date = new Date();

const today = `${
  date.getMonth() + 1 < 10 ? "0" + (date.getMonth() + 1) : date.getMonth()
}${date.getDate() < 10 ? "0" + date.getDate() : date.getDate()}`;

AWS.config.update({
  region: REGION,
});
console.log(`   Upload started.\n`);
let missingCount = 0;

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

  try {
    await dynamodb.batchWrite(req).promise();
  } catch (error) {
    console.error("Error writing to DynamoDB:", error);

    // Write the error message to a new file
    fs.appendFileSync(
      FILE_DIR.replace("/input/", "/output/").replace(".csv", "_log.txt"),
      `Error writing to DynamoDB: ${error}\n\n`,
    );
  }
}

async function checkItemExists(id) {
  const params = {
    TableName: TABLE_NAME,
    Key: {
      ITEM_ID: id,
    },
  };

  const result = await dynamodb.get(params).promise();
  return !!result.Item; // Returns true if item exists, false otherwise
}

function createCSVWriter() {
  const writer = csvWriter({
    headers: columns, // Replace with your column names
  });
  writer.pipe(
    fs.createWriteStream(FILE_DIR.replace("/input/", "/output/"), {
      flags: "a",
    }),
  );
  return writer;
}

function createCSVWriterMissing() {
  const writer = csvWriter({
    headers: columns,
  });
  writer.pipe(
    fs.createWriteStream(
      FILE_DIR.replace("/input/", "/output/").replace(".csv", "_missing.csv"),
      { flags: "a" },
    ),
  );
  return writer;
}

(async function () {
  const readStream = fs.createReadStream(FILE_DIR, { encoding: "utf8" });
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  let firstLine = IGNORE_FIRST_ROW;
  let items = [];
  let batchNo = 0;
  let promises = [];
  let csvWriter;
  let csvWriterMissing;

  if (CHECK_DATA) csvWriter = createCSVWriter();
  if (CHECK_MISSING) csvWriterMissing = createCSVWriterMissing();

  for await (const line of rl) {
    if (firstLine) {
      firstLine = false;
      continue;
    }

    const obj = convertToObject(line);
    if (obj) {
      const id = obj[ITEM_ID];
      let exists = true;
      if (CHECK_MISSING) {
        exists = await checkItemExists(id);
      }
      items.push(obj);
      if (CHECK_DATA) {
        csvWriter.write(obj);
      }
      if (!exists) {
        missingCount++;
        console.log(`
        An item has failed to upload. Please check the log file for more details.
        Missing count: ${missingCount}
        `);
        csvWriterMissing.write(obj);
      }
    }

    if (items.length % batchSize === 0) {
      console.log(`   Batch: ${batchNo}`);
      promises.push(saveToDynamoDB(items));

      if (promises.length % concurrentRequests === 0) {
        console.log("\n   ...Awaiting write requests to DynamoDB...\n");
        console.log(`   ${batchNo * batchSize} items uploaded.`);
        console.log(
          `   ${parseInt(timep / 1000)} seconds elapsed. (${parseInt(
            timep / 60000,
          )} minutes)`,
        );
        await Promise.all(promises);
        promises = [];
      }

      var endp = new Date().getTime();
      var timep = endp - start;
      items = [];
      batchNo++;
    }
  }

  if (items.length > 0) {
    console.log(`   Batch: ${batchNo}`);
    promises.push(saveToDynamoDB(items));
  }

  if (promises.length > 0) {
    console.log("\n   ...Finishing up...\n");
    await Promise.all(promises);
  }

  if (CHECK_DATA) {
    csvWriter.end();
  }
  if (CHECK_MISSING) csvWriterMissing.end();

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
  const now =
    new Date().toISOString().slice(0, 10).replace(/-/g, "") +
    "-" +
    new Date().toString().slice(16, 24).replace(/:/g, "");
  fs.writeFileSync(`./output/upload_log-${TABLE_NAME}-${now}.txt`, message);
  const sleep = (ms = 900000) => new Promise((r) => setTimeout(r, ms));
  sleep();
})();
