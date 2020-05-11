import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getTimeSeriesCommand extends commandBase {
    
    constructor(private docId: string, private timeSeriesName: string, private db: database, 
                private start: number, private pageSize: number, private suppressNotFound = false) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesRangeResult> {
        const args = {
            docId: this.docId,
            name: this.timeSeriesName,
            pageSize: this.pageSize,
            start: this.start
        };
        
        const url = endpoints.databases.timeSeries.timeseries;
        
        return this.query<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesRangeResult>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((result: JQueryXHR) => {
                if (!this.suppressNotFound || result.status !== 404) {
                    this.reportError("Failed to get time series", result.responseText, result.statusText)
                }
            });
    }
}

export = getTimeSeriesCommand;
