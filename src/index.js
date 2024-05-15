// src/index.js

const fs = require("fs");
const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  switch (operator) {
    case "=":
      return row[field] === value;
    case "!=":
      return row[field] !== value;
    case ">":
      return row[field] > value;
    case "<":
      return row[field] < value;
    case ">=":
      return row[field] >= value;
    case "<=":
      return row[field] <= value;
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

async function executeSELECTQuery(query) {
  if (!query.trim().toUpperCase().startsWith("SELECT")) {
    throw new Error("Only SELECT queries are supported");
  }

  const { fields, table, whereClauses, joinTable, joinCondition } =
    parseQuery(query);
  const filePath = `${table}.csv`;

  if (!fs.existsSync(filePath)) {
    throw new Error(`CSV file '${filePath}' not found`);
  }

  console.log(`Reading data from file: ${filePath}`);
  const data = await readCSV(filePath);

  // Perform INNER JOIN if specified
  if (joinTable && joinCondition) {
    const joinFilePath = `${joinTable}.csv`;
    if (!fs.existsSync(joinFilePath)) {
      throw new Error(`CSV file '${joinFilePath}' not found`);
    }

    console.log(`Reading join data from file: ${joinFilePath}`);
    const joinData = await readCSV(joinFilePath);

    data = data.flatMap((mainRow) => {
      return joinData
        .filter((joinRow) => {
          const mainValue = mainRow[joinCondition.left.split(".")[1]];
          const joinValue = joinRow[joinCondition.right.split(".")[1]];
          return mainValue === joinValue;
        })
        .map((joinRow) => {
          return fields.reduce((acc, field) => {
            const [tableName, fieldName] = field.split(".");
            acc[field] =
              tableName === table ? mainRow[fieldName] : joinRow[fieldName];
            return acc;
          }, {});
        });
    });
  }

  // Apply WHERE clause filtering
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Select the specified fields
  const result = filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      // Assuming 'field' is just the column name without table prefix
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });

  console.log(`Query executed successfully. Result:`, result);
  return result;
}

module.exports = executeSELECTQuery;
