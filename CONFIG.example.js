const { v4 } = require("uuid");
const REGION = "ap-southeast-1";
const TABLE_NAME = "table_name";
const FILE_DIR = "./input/test.csv";
const IGNORE_FIRST_ROW = true;
const CHECK_DATA = true; // Export the items extracted from the CSV file.
const CHECK_MISSING = true; // Export the missing items to a new CSV file. This will drastically slow down the upload process
const ITEM_ID = "id"; // The name of the primary key in DynamoDB to check for missing items

// Todo: Fill in columns and schema
const schema = (items) => {
  return {
    id: items[0],
    name: items[1],
    date: items[2],
  };
};

// Code
const columns = Object.keys(schema([]));
function convertToObject(line) {
  const items = line.split(",");
  if (items[0].trim().length > 0) {
    return schema(items);
  } else return null;
}

module.exports = {
  REGION,
  TABLE_NAME,
  FILE_DIR,
  IGNORE_FIRST_ROW,
  convertToObject,
  CHECK_DATA,
  columns,
  CHECK_MISSING,
  ITEM_ID,
};
