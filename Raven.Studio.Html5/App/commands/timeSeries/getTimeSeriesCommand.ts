import timeSeries = require("models/timeSeries/timeSeries");
import commandBase = require("commands/commandBase");

class getTimeSeriesCommand extends commandBase {
    execute(): JQueryPromise<timeSeries[]> {
        var args = {
            getAdditionalData: true
        };
        var url = "/ts";

        var resultsSelector = (ts: timeSeriesDto[]) => 
            ts.map((ts: timeSeriesDto) => new timeSeries(ts.Name, ts.IsAdminCurrentTenant, ts.Disabled, ts.Bundles));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getTimeSeriesCommand;
