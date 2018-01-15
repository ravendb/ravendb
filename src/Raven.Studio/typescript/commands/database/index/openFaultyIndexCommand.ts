import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class openFaultyIndexCommand extends commandBase {

    constructor(private indexNameToOpen: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexNameToOpen
        };

        const url = endpoints.databases.index.indexOpenFaultyIndex + this.urlEncodeArgs(args);
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(`Faulty index ${this.indexNameToOpen} was successfully opened`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to open faulty index: ${this.indexNameToOpen}`, response.responseText, response.statusText));
    }
}

export = openFaultyIndexCommand;
