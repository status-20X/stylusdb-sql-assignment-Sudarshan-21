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

  const { fields, table, whereClauses } = parseQuery(query);
  const filePath = `${table}.csv`;

  if (!fs.existsSync(filePath)) {
    throw new Error(`CSV file '${filePath}' not found`);
  }

  const data = await readCSV(filePath);

  // Apply WHERE clause filtering
  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => evaluateCondition(row, clause))
        )
      : data;

  // Select the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

module.exports = executeSELECTQuery;
