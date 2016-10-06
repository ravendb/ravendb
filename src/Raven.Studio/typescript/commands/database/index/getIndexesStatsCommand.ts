import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesStatsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Data.Indexes.IndexStats[]> {
        const url = endpoints.databases.index.indexesStats;
        return this.query(url, null, this.db);
    }
} 

export = getIndexesStatsCommand;
