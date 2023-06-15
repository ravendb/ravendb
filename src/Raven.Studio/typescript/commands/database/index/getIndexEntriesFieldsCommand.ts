import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexEntriesFieldsCommand extends commandBase {

    private readonly indexName: string;
    private readonly db: database;
    private readonly location: databaseLocationSpecifier;
    private readonly reportFailure: boolean;

    constructor(indexName: string, db: database, location: databaseLocationSpecifier, reportFailure = true) {
        super();
        this.indexName = indexName;
        this.db = db;
        this.location = location;
        this.reportFailure = reportFailure;
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
            op: "entries-fields",
            ...this.location
        };

        const url = endpoints.databases.index.indexesDebug;

        return this.query(url, args, this.db);
    }
}

export = getIndexEntriesFieldsCommand;
