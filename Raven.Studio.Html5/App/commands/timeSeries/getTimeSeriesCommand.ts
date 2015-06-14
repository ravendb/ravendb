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
            ts.map((cs: timeSeriesDto) => new timeSeries(cs.Name, cs.IsAdminCurrentTenant, cs.Disabled, cs.Bundles));
        return this.query(url, args, null, resultsSelector);
    }
}

export = getTimeSeriesCommand;