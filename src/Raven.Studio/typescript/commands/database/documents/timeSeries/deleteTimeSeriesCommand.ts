import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class deleteTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private name: string, private dtos: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.DeleteOperation[], private db: database) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const args = {
            docId: this.documentId
        };
        const url = endpoints.databases.timeSeries.timeseries + this.urlEncodeArgs(args);

        const payload: TimeSeriesOperation = {
            Name: this.name, 
            Appends: [],
            Deletes: this.dtos,
            Increments: []
        };

        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete time series.", response.responseText, response.statusText));
    }
}

export = deleteTimeSeriesCommand;
