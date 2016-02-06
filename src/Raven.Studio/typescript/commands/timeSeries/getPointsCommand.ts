import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesPoint = require("models/timeSeries/timeSeriesPoint");

class getPointsCommand extends commandBase {

    /**
    * @param skip - number of entries to skip
    * @param take - number of entries to take
    */
    constructor(private ts: timeSeries, private skip: number, private take: number, private type: string, private fields: string[],
        private key: string, private start: string, private end: string) {
        super();
    }

    execute(): JQueryPromise<timeSeriesPoint[]> {
        var url = "/points/" + this.type + "?key=" + this.key;
        var selector = (dtos: pointDto[]) => dtos.map(d => new timeSeriesPoint(this.type, this.fields, this.key, d.At, d.Values));
        var args = {
            skip: this.skip,
            take: this.take,
            start: this.start,
            end: this.end
        };
        return this.query(url, args, this.ts, selector);
    }
}

export = getPointsCommand;
