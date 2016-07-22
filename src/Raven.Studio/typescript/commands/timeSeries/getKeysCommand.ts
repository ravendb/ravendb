import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import pagedResultSet = require("common/pagedResultSet");

class getKeysCommand extends commandBase {

    constructor(private ts: timeSeries, private skip: number, private take: number, private type: string, private keysCount: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet<timeSeriesKey>> {
        var doneTask = $.Deferred();
        var selector = (keys: timeSeriesKeyDto[]) => keys.map((key: timeSeriesKeyDto) => new timeSeriesKey(key, this.ts));
        var task = this.query("/keys/" + this.type, {
            skip: this.skip,
            take: this.take
        }, this.ts, selector);
        task.done((keys: timeSeriesKey[]) => doneTask.resolve(new pagedResultSet(keys, this.keysCount)));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }   
}

export = getKeysCommand; 
