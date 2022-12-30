import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesStatsCommand extends commandBase {

    private db: database;

    private location?: databaseLocationSpecifier;

    constructor(db: database, location?: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexStats[]> {
        const url = endpoints.databases.index.indexesStats;
        const args = this.location;
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexStats>) => response.Results;
        return this.query(url, args, this.db, extractor);
    }
}

export = getIndexesStatsCommand;
