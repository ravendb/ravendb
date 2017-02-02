import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIOMetricsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.IOMetricsResponse> {
        const url = endpoints.databases.ioMetrics.debugIoMetrics;
        return this.query<Raven.Server.Documents.Handlers.IOMetricsResponse>(url, null, this.db);
    }
}

export = getIOMetricsCommand;
