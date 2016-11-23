import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class enableIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName
        }
        this.reportInfo("Enabling " + this.indexName);
        const url = endpoints.databases.adminIndex.adminIndexesEnable + this.urlEncodeArgs(args);
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                this.reportSuccess("Enabled " + this.indexName + ".");
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to enable index", response.responseText));
    }
}

export = enableIndexCommand; 
