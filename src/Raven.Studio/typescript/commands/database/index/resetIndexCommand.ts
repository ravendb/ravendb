import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class resetIndexCommand extends commandBase {

    constructor(private indexNameToReset: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<{ IndexId: number }> {
        const args = {
            name: this.indexNameToReset
        };
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);
        return this.reset(url, null, this.db)
            .done(() => this.reportSuccess("Index " + this.indexNameToReset + " successfully reset"))
            .fail((response: JQueryXHR) => this.reportError("Failed to reset index: " + this.indexNameToReset, response.responseText, response.statusText));
    }

}


export = resetIndexCommand;
