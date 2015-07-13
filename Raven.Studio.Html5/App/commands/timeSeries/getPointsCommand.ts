import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeriesDocument");
import pagedResultSet = require("common/pagedResultSet");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class getPointsCommand extends commandBase {

    /**
    * @param timeSeriesStorage - the timeSeries storage that is being used
    * @param skip - number of entries to skip
    * @param take - number of entries to take
    * @param timeSeriesGroupName - the timeSeries group to take the entries from
    */
    constructor(private ts: timeSeries, private skip: number, private take: number, private prefix: string, private key: string) {
        super();
    }

    execute(): JQueryPromise<pagedResultSet> {
        var url = "/" + this.prefix + "/" + this.key + "/points";
        var doneTask = $.Deferred();
        var selector = (dtos: pointDto[]) => dtos.map(d => new timeSeriesPoint(this.prefix, this.key, d));
        var task = this.query(url, {
            skip: this.skip,
            take: this.take
        }, this.ts, selector);
        task.done((summaries: pointDto[]) => doneTask.resolve(new pagedResultSet(summaries, summaries.length)));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getPointsCommand;  