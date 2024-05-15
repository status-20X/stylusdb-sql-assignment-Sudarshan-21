// src/index.js

const fs = require("fs");
const parseQuery = require("./queryParser");
const readCSV = require("./csvReader");

async function executeSELECTQuery(query) {
  if (!query.trim().toUpperCase().startsWith("SELECT")) {
    throw new Error("Only SELECT queries are supported");
  }

  const { fields, table } = parseQuery(query);
  const filePath = `${table}.csv`;

  if (!fs.existsSync(filePath)) {
    throw new Error(`CSV file '${filePath}' not found`);
  }

  const data = await readCSV(filePath);

  // Filtering based on WHERE clause
  const filteredData = whereClause
    ? data.filter((row) => {
        const [field, value] = whereClause.split("=").map((s) => s.trim());
        return row[field] === value;
      })
    : data;

  // Selecting the specified fields
  return filteredData.map((row) => {
    const selectedRow = {};
    fields.forEach((field) => {
      selectedRow[field] = row[field];
    });
    return selectedRow;
  });
}

module.exports = executeSELECTQuery;
