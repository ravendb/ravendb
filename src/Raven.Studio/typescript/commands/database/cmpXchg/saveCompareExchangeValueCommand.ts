import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveCompareExchangeValueCommand extends commandBase {

    constructor(private database: database, private key: string, private index: number, private data: any) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>> {
        const args = {
            key: this.key,
            index: this.index
        };
        const payload = {
            Object: this.data
        };
        
        const url = endpoints.databases.compareExchange.cmpxchg + this.urlEncodeArgs(args);
        return this.put<Raven.Client.Documents.Operations.CompareExchange.CompareExchangeResult<any>>(url, JSON.stringify(payload), this.database)
            .fail((response: JQueryXHR) => this.reportError("Failed to save compare exchange value", response.responseText, response.statusText));
    }

}

export = saveCompareExchangeValueCommand;
