import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class loadDatabaseCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const url = endpoints.databases.stats.stats;
        return this.query<void>(url, null, this.db, null, null, this.getTimeToAlert(true));
    }
}

export = loadDatabaseCommand;
