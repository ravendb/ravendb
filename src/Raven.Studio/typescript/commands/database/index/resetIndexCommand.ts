import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class resetIndexCommand extends commandBase {

    constructor(private indexNameToReset: string, private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<{ IndexId: number }> {
        const args = {
            name: this.indexNameToReset, 
            ...this.location
        };
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);
        return this.reset<{ IndexId: number }>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to reset index: " + this.indexNameToReset, response.responseText, response.statusText));
    }
}

export = resetIndexCommand;
