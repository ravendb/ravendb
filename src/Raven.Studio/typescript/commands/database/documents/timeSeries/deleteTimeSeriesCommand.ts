import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class deleteTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private dtos: Raven.Client.Documents.Operations.TimeSeries.RemoveTimeSeriesOperation[], private db: database) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.timeSeries.timeseries;

        const payload = {
            Id: this.documentId,
            Removals: this.dtos
        } as Raven.Client.Documents.Operations.TimeSeries.DocumentTimeSeriesOperation;

        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete time series.", response.responseText, response.statusText));
    }
}

export = deleteTimeSeriesCommand;
