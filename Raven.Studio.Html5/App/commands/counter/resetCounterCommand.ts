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
        return this.post(url, args, this.cs);
    }
}

export = resetCounterCommand;  