import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesType = require("models/timeSeries/timeSeriesType");

class getTypesCommand extends commandBase {

    constructor(private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<timeSeriesType[]> {
        var selector = (keys: timeSeriesTypeDto[]) => keys.map((key: timeSeriesTypeDto) => timeSeriesType.fromDto(key, this.ts));
        return this.query("/types", {
            skip: 0,
            take: 1024
        }, this.ts, selector);
    }
}

export = getTypesCommand; 