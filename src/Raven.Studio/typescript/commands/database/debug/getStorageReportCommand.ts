import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getStorageReportCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<storageReportItem[]> {
        const url = endpoints.databases.storage.debugStorageReport;
        return this.query<storageReportItem[]>(url, null, this.db);
    }
}

export = getStorageReportCommand;
