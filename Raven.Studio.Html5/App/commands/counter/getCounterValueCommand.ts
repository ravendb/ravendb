import commandBase = require("commands/commandBase");
import database = require("models/database");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");
import appUrl = require("common/appUrl");

class getCounterValueCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private storage: counterStorage, private counterToReceive:counter) {
        super();

    }

    execute(): JQueryPromise<counter> {
        var args = {
            counterName: this.counterToReceive.id(),
            group: this.counterToReceive.group()
        };

        var url = "/getCounterValue";
        var selector = (dto: counterDto) => new counter(dto);
        return this.query(url, args, this.storage, selector);
    }
}

export = getCounterValueCommand;  


//getCounterValue 