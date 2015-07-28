import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeriesDocument");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import pagedResultSet = require("common/pagedResultSet");

class getKeysCommand extends commandBase {

    constructor(private ts: timeSeries, private skip: number, private take: number, private type: string, private keysCount: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var doneTask = $.Deferred();
        var selector = (keys: timeSeriesKeyDto[]) => keys.map((key: timeSeriesKeyDto) => new timeSeriesKey(key));
        var task = this.query("/" + this.type + "/keys", {
            skip: this.skip,
            take: this.take
        }, this.ts, selector);
        task.done((keys: timeSeriesKeyDto[]) => doneTask.resolve(new pagedResultSet(keys, this.keysCount)));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getKeysCommand; 