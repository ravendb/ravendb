import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIOMetricsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<ioMetricsResponse> {
        const url = endpoints.databases.ioMetrics.debugIoMetrics;
        return this.query<ioMetricsResponse>(url, null, this.db);
    }
}

export = getIOMetricsCommand;
