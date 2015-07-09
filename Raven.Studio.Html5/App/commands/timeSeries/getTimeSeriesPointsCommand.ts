import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import pagedResultSet = require("common/pagedResultSet");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class getTimeSeriesPointsCommand extends commandBase {

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
        var args = {
            skip: this.skip,
            take: this.take,
            prefix: this.prefix,
            key: this.key
        };

        var url = "/timeSeries";
        var doneTask = $.Deferred();
        var selector = (dtos: timeSeriesPointDto[]) => dtos.map(d => new timeSeriesPoint(d));
        var task = this.query(url, args, this.ts, selector);
        task.done((summaries: timeSeriesPointDto[]) => doneTask.resolve(new pagedResultSet(summaries, summaries.length)));
        task.fail(xhr => doneTask.reject(xhr));
        return doneTask;
    }
}

export = getTimeSeriesPointsCommand;  