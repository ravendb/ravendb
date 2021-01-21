import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCompareExchangeItemCommand extends commandBase {

    constructor(private database: database, private key: string, private index: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>> {
        const args = {
            key: this.key,
            index: this.index
        };

        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.del<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>>(url, null, this.database)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete Compare Exchange item", response.responseText, response.statusText));
    }
}

export = deleteCompareExchangeItemCommand;
