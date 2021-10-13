import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import timeSeriesEntryModel = require("models/database/timeSeries/timeSeriesEntryModel");

class saveTimeSeriesCommand extends commandBase {
    constructor(private documentId: string, private name: string,
                private dto: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation, private db: database) {
        super();
    }
    
    execute(): JQueryPromise<void> {
        const args = {
            docId: this.documentId
        };

        const url = endpoints.databases.timeSeries.timeseries + this.urlEncodeArgs(args);
        
        const isRollup = timeSeriesEntryModel.isRollupName(this.name);
        const isIncremental = timeSeriesEntryModel.isIncrementalName(this.name);
        
        const payload: TimeSeriesOperation = {
            Name: this.name,
            Deletes: [],
            Appends: isRollup || !isIncremental ? [this.dto] : [],
            Increments: !isRollup && isIncremental ? [this.dto] : []
        };
        
        return this.post(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError("Failed to save time series.", response.responseText, response.statusText));
    }
}

export = saveTimeSeriesCommand;
