const mysql = require("mysql");
const util = require("util");

const connectionInfo = {
    host: "[server_host]",
    user: "[username]",
    password: "[password]",
    database: "[database]"
};

const executeQuery = async (query, connectionData = connectionInfo) => {
    const connection = mysql.createPool(connectionData);
    //connection.connect();
    const queryPromise = util.promisify(connection.query).bind(connection);    
    const result = await queryPromise(query);
    //connection.end();
    return result;
}

module.exports = { executeQuery };