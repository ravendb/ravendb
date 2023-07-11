import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexMergeSuggestionsCommand extends commandBase {

    private readonly db: database;

    constructor(db: database) {
        super();
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Server.Documents.Indexes.IndexMerging.IndexMergeResults> {
        const url = endpoints.databases.index.indexesSuggestIndexMerge;
        return this.query(url, null, this.db);
    }
}

export = getIndexMergeSuggestionsCommand;
