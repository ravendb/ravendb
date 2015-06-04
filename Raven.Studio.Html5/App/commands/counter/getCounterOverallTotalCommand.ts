import commandBase = require("commands/commandBase");
import counterChange = require("models/counter/counterChange");
import counterStorage = require("models/counter/counterStorage");

class getCounterOverallTotalCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private storage: counterStorage, private counterToReceive: counterChange) {
        super();

    }

    execute(): JQueryPromise<counterChange> {
        var args = {
            group: this.counterToReceive.group(),
            counterName: this.counterToReceive.counterName()
        };

        var url = "/getCounterOverallTotal";
        var selector = (dto: counterDto) => new counterChange(dto);
        return this.query(url, args, this.storage, selector);
    }
}

export = getCounterOverallTotalCommand;