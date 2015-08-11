import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");

class getKeyCommand extends commandBase {

    constructor(private ts: timeSeries, private type: string, private key: string) {
        super();
    }

    execute(): JQueryPromise<timeSeriesKey> {
        var doneTask = $.Deferred();
        var selector = (key: timeSeriesKeyDto) => new timeSeriesKey(key, this.ts);
        var task = this.query("/key/" + this.type + "?key=" + this.key, null, this.ts, selector);
        task.done((key: timeSeriesKeyDto) => doneTask.resolve(key));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getKeyCommand;