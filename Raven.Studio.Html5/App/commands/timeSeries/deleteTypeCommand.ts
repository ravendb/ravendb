import commandBase = require("commands/commandBase");
import timeSeries = require("models/timeSeries/timeSeries");

class deleteTypeCommand extends commandBase {

    constructor(private type: string, private ts: timeSeries) {
        super();
    }

    execute(): JQueryPromise<any> {
        var task = this.del("/types/" + this.type, null, this.ts, { dataType: undefined })
            .done(() => this.reportSuccess(`Successfully deleted '${this.type}' type`))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete '" + this.type + "' type", response.responseText, response.statusText));

        return task;
    }
}

export = deleteTypeCommand;  