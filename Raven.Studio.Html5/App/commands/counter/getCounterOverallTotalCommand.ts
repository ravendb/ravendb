import commandBase = require("commands/commandBase");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");

class getCounterOverallTotalCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private storage: counterStorage, private counterToReceive:counter) {
        super();

    }

    execute(): JQueryPromise<counter> {
        var args = {
            group: this.counterToReceive.group(),
            counterName: this.counterToReceive.id()
        };

        var url = "/getCounterOverallTotal";
        var selector = (dto: counterDto) => new counter(dto);
        return this.query(url, args, this.storage, selector);
    }
}

export = getCounterOverallTotalCommand;