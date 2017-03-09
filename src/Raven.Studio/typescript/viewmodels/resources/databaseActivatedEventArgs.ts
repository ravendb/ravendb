import database = require("models/resources/database");

interface databaseActivatedEventArgs {
    database: database;
}

export = databaseActivatedEventArgs;