import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexEntriesFieldsCommand extends commandBase {

    constructor(private indexName: string, private db: database, private reportFailure: boolean = true) {
        super();
    }

    execute(): JQueryPromise<getIndexEntriesFieldsCommandResult> {
        const task = this.getIndexEntries();

        if (this.reportFailure) {
            task.fail((response: JQueryXHR) => {
                this.reportError("Failed to get index entries", response.responseText, response.statusText);
            });
        }

        return task;
    }

    private getIndexEntries(): JQueryPromise<getIndexEntriesFieldsCommandResult> {
        const args = {
            name: this.indexName,
            op: "entries-fields"
        };

        const url = endpoints.databases.index.indexesDebug;

        return this.query(url, args, this.db);
    }
}

export = getIndexEntriesFieldsCommand;
