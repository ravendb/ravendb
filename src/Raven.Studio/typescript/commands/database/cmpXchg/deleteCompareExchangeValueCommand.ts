import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCompareExchangeValueCommand extends commandBase {

    constructor(private database: database, private key: string, private index: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>> {
        const args = {
            key: this.key,
            index: this.index
        };

        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.del(url, null, this.database);
        // don't handle failure here
    }

}

export = deleteCompareExchangeValueCommand;
