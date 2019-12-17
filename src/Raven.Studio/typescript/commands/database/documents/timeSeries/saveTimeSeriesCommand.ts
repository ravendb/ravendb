import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class saveTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private dto: Raven.Client.Documents.Operations.TimeSeries.AppendTimeSeriesOperation, 
                private db: database, private forceOverride: boolean = false) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const url = endpoints.databases.timeSeries.timeseries;
        
        const payload = {
            Id: this.documentId,
            Removals: this.forceOverride ? [
                    {
                        From: this.dto.Timestamp,
                        To: this.dto.Timestamp,
                        Name: this.dto.Name
                    }
                ] : [],
            Appends: [
                this.dto
            ]
        } as Raven.Client.Documents.Operations.TimeSeries.DocumentTimeSeriesOperation;
        
        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to save time series.", response.responseText, response.statusText));
    }
}

export = saveTimeSeriesCommand;
