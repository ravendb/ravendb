import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeriesDocument");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");

class getTimeSeriesKeysCommand extends commandBase {

    constructor(private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<timeSeriesKey[]> {
        var selector = (keys: timeSeriesKeyDto[]) => keys.map((key: timeSeriesKeyDto) => timeSeriesKey.fromDto(key, this.ts));
        return this.query("/keys", null, this.ts, selector);
    }
}

export = getTimeSeriesKeysCommand; 