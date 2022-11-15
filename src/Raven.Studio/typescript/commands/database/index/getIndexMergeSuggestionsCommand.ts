import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexMergeSuggestionsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults> {
        const url = endpoints.databases.index.indexesSuggestIndexMerge;
        return this.query(url, null, this.db);
    }
}

export = getIndexMergeSuggestionsCommand;
