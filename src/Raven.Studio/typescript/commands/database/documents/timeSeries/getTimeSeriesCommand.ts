import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");
import moment = require("moment");


class getTimeSeriesCommand extends commandBase {
    
    constructor(private docId: string, private timeSeriesName: string, private db: database, 
                private start: number, private pageSize: number, private suppressNotFound = false,
                private startDateLocal?: moment.Moment, private endDateLocal?: moment.Moment) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TimeSeries.TimeSeriesRangeResult> {
        const args = {
            docId: this.docId,
            name: this.timeSeriesName,
            pageSize: this.pageSize,
            start: this.start,
            full: timeSeriesEntryModel.isIncrementalName(this.timeSeriesName),
            from: this.startDateLocal?.clone().utc().format(),
            to: this.endDateLocal?.clone().utc().format()
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
