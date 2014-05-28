import commandBase = require("commands/commandBase");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");

class resetCounterCommand extends commandBase {

    /**
    * @param counterStorage - the counter storage that is being used
    * @param editedCounter - the edited counter
    */
    constructor(private storage: counterStorage, private counterToReset: counter) {
        super();
    }

    execute(): JQueryPromise<counter[]> {
        var args = {
            counterName: this.counterToReset.id(),
            group: this.counterToReset.group(),
        };

        var url = "/reset" + this.urlEncodeArgs(args);
        return this.post(url, null, this.storage, { dataType: undefined });
    }
}

export = resetCounterCommand;  