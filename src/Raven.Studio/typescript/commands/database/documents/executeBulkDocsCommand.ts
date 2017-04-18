import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class executeBulkDocsCommand extends commandBase {
    constructor(public docs: Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[], private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[]> {
        return this.post<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData[]>(endpoints.databases.batch.bulk_docs, JSON.stringify(this.docs), this.db);
    }
}

export = executeBulkDocsCommand; 
