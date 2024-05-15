// src/index.js

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

  // Filter the fields based on the query
  return data.map((row) => {
    const filteredRow = {};
    fields.forEach((field) => {
      filteredRow[field] = row[field];
    });
    return filteredRow;
  });
}

module.exports = executeSELECTQuery;
