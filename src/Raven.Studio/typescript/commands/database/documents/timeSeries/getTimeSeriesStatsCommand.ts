import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getTimeSeriesStatsCommand extends commandBase {
    
    constructor(private docId: string, private db: database) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesStatistics> {
        const args = {
            docId: this.docId
        };
        
        const url = endpoints.databases.timeSeries.timeseriesStats;
        
        return this.query<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesStatistics>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((result: JQueryXHR) => this.reportError("Failed to get time series details", result.responseText, result.statusText));
    }
}

export = getTimeSeriesStatsCommand;
