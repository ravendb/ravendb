import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import indexProgress = require("models/database/index/indexProgress");

class computeIndexingProgressCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<indexProgress> {
        const args = {
            name: this.indexName
        };
        const url = endpoints.databases.index.indexesProgress;
        return this.query(url, args, this.db, dto => new indexProgress(dto))
            .fail((response: JQueryXHR) => this.reportError("Failed to compute indexing progress!", response.responseText, response.statusText));
    }
} 

export = computeIndexingProgressCommand;
