import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class disableIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName, 
        }
        this.reportInfo("Disabling " + this.indexName);
        const url = endpoints.databases.adminIndex.adminIndexesDisable + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess("Disabled " + this.indexName + ".");
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to disable index", response.responseText));
    }
}

export = disableIndexCommand; 
