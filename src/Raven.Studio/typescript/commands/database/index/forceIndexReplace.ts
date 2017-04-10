import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class forceIndexReplace extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName
        }
        const url = endpoints.databases.index.indexesReplace + this.urlEncodeArgs(args);

        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => this.reportSuccess("Replaced index " + this.indexName))
            .fail((response: JQueryXHR) => this.reportError("Failed to replace index.", response.responseText, response.statusText));
    }
}

export = forceIndexReplace; 
