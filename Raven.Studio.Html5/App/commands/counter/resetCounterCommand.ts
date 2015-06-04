import commandBase = require("commands/commandBase");
import counterStorage = require("models/counter/counterStorage");

class resetCounterCommand extends commandBase {

    /**
    * @param counterStorage - the counter storage that is being used
    * @param editedCounter - the edited counter
    */
    constructor(private cs: counterStorage, private group: string, private counterName: string) {
        super();
    }

    execute(): JQueryPromise<any> {
        var args = {
            group: this.group,
            counterName: this.counterName
        };

        var url = "/reset" + this.urlEncodeArgs(args);
        return this.post(url, null, this.cs, { dataType: undefined });
    }
}

export = resetCounterCommand;  