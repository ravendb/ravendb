import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexDebugSourceDocumentsCommand extends commandBase {
    constructor(private db: database, private location: databaseLocationSpecifier, private indexName: string, private startsWith: string, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<arrayOfResultsAndCountDto<string>> {
        const args = {
            start: this.skip,
            pageSize: this.take,
            op: "source-doc-ids",
            name: this.indexName,
            startsWith: this.startsWith,
            ...this.location
        };

        const url = endpoints.databases.index.indexesDebug;
        return this.query(url, args, this.db);
    }
}

export = getIndexDebugSourceDocumentsCommand;
