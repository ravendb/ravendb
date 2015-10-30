import commandBase = require("commands/commandBase");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");

class updateCounterCommand extends commandBase {

    /**
    * @param counterStorage - the counter storage that is being used
    * @param editedCounter - the edited counter
    * @param delta - the change to apply to the counter
    */
    constructor(private storage: counterStorage, private editedCounter: counter, private delta: number) {
        super();
    }

    execute(): JQueryPromise<counter[]> {
        var args = {
            counterName: this.editedCounter.id(),
            group: this.editedCounter.group(),
            delta: this.delta
        };

        var url = "/change" + this.urlEncodeArgs(args);
        return this.post(url, null, this.storage, { dataType: undefined });
    }
}

export = updateCounterCommand;  
