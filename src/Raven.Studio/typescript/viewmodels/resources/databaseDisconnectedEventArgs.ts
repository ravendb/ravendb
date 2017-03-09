import database = require("models/resources/database");

interface databaseDisconnectedEventArgs {
    database: database;
    cause: databaseDisconnectionCause;
}

export = databaseDisconnectedEventArgs;