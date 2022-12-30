import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteIndexCommand extends commandBase {
    private indexName: string;

    private db: database;

    constructor(indexName: string, db: database) {
        super();
        this.db = db;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<any> {
        const args = {
            name: this.indexName
        };

        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);

        return this.del(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to delete index " + this.indexName, response.responseText))
            .done(() => this.reportSuccess("Deleted " + this.indexName));
    }
}

export = deleteIndexCommand;
