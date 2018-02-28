import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getCompareExchangeValueCommand extends commandBase {

    constructor(private database: database, private key: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeValue<any>> {
        const args = {
            key: this.key
        };

        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.query(url, null, this.database, x => x.Results[0]);
    }

}

export = getCompareExchangeValueCommand;
