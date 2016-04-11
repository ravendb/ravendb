import timeSeries = require("models/timeSeries/timeSeries");
import commandBase = require("commands/commandBase");

class getTimeSeriesCommand extends commandBase {
    execute(): JQueryPromise<timeSeries[]> {
        var args = {
            pageSize: 1024,
            getAdditionalData: true
        };
        var url = "/ts";

        var resultsSelector = (ts: timeSeriesDto[]) => 
            ts.map((ts: timeSeriesDto) => new timeSeries(ts.Name, ts.IsAdminCurrentTenant, ts.Disabled));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getTimeSeriesCommand;
