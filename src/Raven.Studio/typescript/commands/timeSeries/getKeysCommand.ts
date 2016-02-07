import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");
import pagedResultSet = require("common/pagedResultSet");

class getKeysCommand extends commandBase {

    constructor(private ts: timeSeries, private skip: number, private take: number, private type: string, private keysCount: number) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var selector = (keys: timeSeriesKeyDto[]) => keys.map((key: timeSeriesKeyDto) => new timeSeriesKey(key, this.ts));
        var doneTask = $.Deferred();
        var args = {
            skip: this.skip,
            take: this.take
        };

        var task = this.query("/keys/" + this.type, args, this.ts, selector);
        task.done((summaries: timeSeriesKey[]) => doneTask.resolve(new pagedResultSet(summaries, summaries.length)));
        task.fail(xhr => doneTask.reject(xhr));

        doneTask.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getKeysCommand; 
