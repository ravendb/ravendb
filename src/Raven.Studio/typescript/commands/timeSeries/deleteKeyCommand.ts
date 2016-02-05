import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class deleteKeyCommand extends commandBase {

    constructor(private type: string, private key: string, private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            key: this.key
        };
        var url = "/delete-key/" + this.type + this.urlEncodeArgs(args);
        return this.del(url, null, this.ts, { dataType: undefined })
            .done((numOfDeletedPoints: number) => this.reportSuccess(`Successfully deleted '${this.key}' key, with ${numOfDeletedPoints} points`))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete '" + this.key + "' key", response.responseText, response.statusText));
    }
}

export = deleteKeyCommand;
