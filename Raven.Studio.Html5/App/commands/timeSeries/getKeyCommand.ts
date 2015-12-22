import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");

class getKeyCommand extends commandBase {

    constructor(private ts: timeSeries, private type: string, private key: string) {
        super();
    }

    execute(): JQueryPromise<timeSeriesKey> {
        var doneTask = $.Deferred();
        var task = this.query("/key/" + this.type + "?key=" + this.key, null, this.ts);
        task.done((key: timeSeriesKeySummaryDto) => doneTask.resolve(key));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getKeyCommand;
