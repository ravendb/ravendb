import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class resetCounterCommand extends commandBase {

    constructor(private cs: counterStorage, private groupName: string, private counterName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            groupName: this.groupName,
            counterName: this.counterName
        };

        var url = "/reset" + this.urlEncodeArgs(args);
        var action = this.post(url, args, this.cs, { dataType: undefined });
        action.done(() => this.reportSuccess("Successfully reset counter!"));
        action.fail((response: JQueryXHR) => this.reportError("Failed to reset counter!", response.responseText, response.statusText));
        return action;
    }
}

export = resetCounterCommand;  
