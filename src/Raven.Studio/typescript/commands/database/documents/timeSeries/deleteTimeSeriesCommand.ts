import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class deleteTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private name: string, private dtos: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.RemoveOperation[], private db: database) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.timeSeries.timeseries;

        const payload = {
            DocumentId: this.documentId,
            Name: this.name, 
            Removals: this.dtos
        } as Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation;

        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete time series.", response.responseText, response.statusText));
    }
}

export = deleteTimeSeriesCommand;
