import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");
import timeSeriesKey = require("models/timeSeries/timeSeriesKey");

class deleteKeyCommand extends commandBase {

    constructor(private key: timeSeriesKey, private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            key: this.key.Key
        };
        var url = "/delete-key/" + this.key.Type + this.urlEncodeArgs(args);
        return this.del(url, null, this.ts, { dataType: undefined }, (9000 * this.key.Points) / 2)
            .done((numOfDeletedPoints: number) => this.reportSuccess(`Successfully deleted '${this.key.Key}' key, with ${numOfDeletedPoints} points`))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete '" + this.key.Key + "' key", response.responseText, response.statusText));
    }
}

export = deleteKeyCommand;  