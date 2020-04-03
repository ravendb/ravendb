import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private name: string, private dto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation, 
                private db: database, private forceOverride: boolean = false) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.timeSeries.timeseries;
        
        const payload = {
            DocumentId: this.documentId,
            Name: this.name,
            Removals: this.forceOverride ? [
                    {
                        From: this.dto.Timestamp,
                        To: this.dto.Timestamp,
                    }
                ] : [],
            Appends: [
                this.dto
            ]
        } as Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation;
        
        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to save time series.", response.responseText, response.statusText));
    }
}

export = saveTimeSeriesCommand;
