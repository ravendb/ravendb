/*import commandBase = require("commands/commandBase");
import pointChange = require("models/timeSeries/timeSeriesChange");
import timeSeries = require("models/timeSeries/timeSeries");

class addPointCommand extends commandBase {

    constructor(private ts: timeSeries, private type: string, private key: string, private at: number, private values: number[]) {
        super();
    }

    execute(): JQueryPromise<timeSeriesChange[]> {
        var args = {
            group: this.group,
            timeSeriesName: this.timeSeriesName,
            delta: this.delta
        };
        var url = "/change/" + this.group + "/" + this.timeSeriesName + this.urlEncodeArgs({delta: this.delta });
        var action = this.post(url, null, this.ts, { dataType: undefined });

        var successMessage = this.isNew ? "Successfully created a new timeSeries!" : "Successfully updated a timeSeries!";
        action.done(() => this.reportSuccess(successMessage));
        var failMessage = this.isNew ? "Failed to create a new timeSeries!" : "Successfully to update timeSeries!";
        action.fail((response: JQueryXHR) => this.reportError(failMessage, response.responseText, response.statusText));
        return action;
    }
}

export = addPointCommand;  */