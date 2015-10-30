import commandBase = require("commands/commandBase");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");

class getCounterCommand extends commandBase {

    constructor(private cs: counterStorage, private groupName: string, private counterName: string) {
        super();

    }

    execute(): JQueryPromise<counter> {
        var args = {
            groupName: this.groupName,
            counterName: this.counterName
        };

        var url = "/getCounter";
        var selector = (dto: counterDto) => new counter(dto);
        return this.query(url, args, this.cs, selector);
    }
}

export = getCounterCommand;
