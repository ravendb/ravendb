import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeriesDocument");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");

class getKeysCommand extends commandBase {

    constructor(private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<timeSeriesKey[]> {
        var selector = (keys: timeSeriesKeyDto[]) => keys.map((key: timeSeriesKeyDto) => timeSeriesKey.fromDto(key, this.ts));
        return this.query("/" + this.type + "/keys", null, this.ts, selector);
    }
}

export = getKeysCommand; 