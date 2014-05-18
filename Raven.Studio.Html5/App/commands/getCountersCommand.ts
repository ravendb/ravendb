import commandBase = require("commands/commandBase");
import database = require("models/database");
import counter = require("models/counter");
import appUrl = require("common/appUrl");

class getCountersCommand extends commandBase {

    /**
    * @param ownerDb The database the collections will belong to.
    */
    constructor(private skip: number, private take: number, private counterGroupName: string) {
        super();

    }

    execute(): JQueryPromise<counter[]> {
        var args = {
            skip: this.skip,
            take: this.take,
            counterGroupName: this.counterGroupName
        };

        var url = "/counters/test/counters" + this.urlEncodeArgs(args);
        var selector = (dtos: any/*counterDto[]*/) => dtos.map(d => new counter(d));
        return this.query(url, null, appUrl.getSystemDatabase(), selector);
    }
}

export = getCountersCommand;  