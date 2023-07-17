const AWS = require('aws-sdk');
const { REGION, TABLE_NAME } = require('./CONFIG');

AWS.config.update({
  region: REGION,
});

const dynamodb = new AWS.DynamoDB();

async function createTable() {
  const params = {
    TableName: TABLE_NAME,
    KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
    AttributeDefinitions: [
      { AttributeName: 'date', AttributeType: 'S' },
      { AttributeName: 'email', AttributeType: 'S' },
      { AttributeName: 'Cohort', AttributeType: 'S' },
    ],
    BillingMode: 'PAY_PER_REQUEST',
  };

  await dynamodb.createTable(params).promise();
}

(async function () {
  await createTable();
})();
