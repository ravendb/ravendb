import commandBase = require("commands/commandBase");
import counterChange = require("models/counter/counterChange");
import counterStorage = require("models/counter/counterStorage");

class updateCounterCommand extends commandBase {

    constructor(private cs: counterStorage, private group: string, private counterName: string, private delta: string, private isNew: boolean) {
        super();
    }

    execute(): JQueryPromise<counterChange[]> {
        var args = {
            group: this.group,
            counterName: this.counterName,
            delta: this.delta
        };
        var url = "/change/" + this.group + "/" + this.counterName + this.urlEncodeArgs({delta: this.delta });
        var action = this.post(url, null, this.cs, { dataType: undefined });

        var successMessage = this.isNew ? "Successfully created a new counter!" : "Successfully updated a counter!";
        action.done(() => this.reportSuccess(successMessage));
        var failMessage = this.isNew ? "Failed to create a new counter!" : "Successfully to update counter!";
        action.fail((response: JQueryXHR) => this.reportError(failMessage, response.responseText, response.statusText));
        return action;
    }
}

export = updateCounterCommand;  