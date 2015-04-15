import commandBase = require("commands/commandBase");
import counter = require("models/counter/counter");
import counterStorage = require("models/counter/counterStorage");

class getCountersCommand extends commandBase {

    /**
    * @param counterStorage - the counter storage that is being used
    * @param skip - number of entries to skip
    * @param take - number of entries to take
    * @param counterGroupName - the counter group to take the entries from
    */
    constructor(private storage: counterStorage, private skip: number, private take: number, private counterGroupName?: string) {
        super();
    }

    execute(): JQueryPromise<counter[]> {
        var args = {
            skip: this.skip,
            take: this.take,
            counterGroupName: this.counterGroupName
        };

        var url = "/counters";
        var selector = (dtos: counterDto[]) => dtos.map(d => new counter(d));
        return this.query(url, args, this.storage, selector);
    }
}

export = getCountersCommand;  