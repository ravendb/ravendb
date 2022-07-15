import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesStatsCommand extends commandBase {

    constructor(private db: database, private location?: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexStats[]> {
        const url = endpoints.databases.index.indexesStats;
        const args = this.location;
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexStats>) => response.Results;
        return this.query(url, args, this.db, extractor);
    }
}

export = getIndexesStatsCommand;
