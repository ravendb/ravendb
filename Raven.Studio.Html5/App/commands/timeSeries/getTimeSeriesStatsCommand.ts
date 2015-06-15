import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class getTimeSeriesStatsCommand extends commandBase {

    constructor(private cs: timeSeries, private longWait: boolean = false) {
        super();
    }

    execute(): JQueryPromise<timeSeriesStatisticsDto> {
        var url = "/stats";
        return this.query<timeSeriesStatisticsDto>(url, null, this.cs, null, this.getTimeToAlert(this.longWait));
    }
}

export = getTimeSeriesStatsCommand;