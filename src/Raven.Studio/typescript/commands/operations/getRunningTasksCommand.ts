import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getRunningTasksCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.PendingOperation[]> {
        const url = endpoints.databases.operations.operations;
        return this.query<Raven.Server.Documents.PendingOperation[]>(url, null, this.db);
    }
}

export = getRunningTasksCommand;
