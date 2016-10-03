import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesPerformance extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Data.Indexes.IndexPerformanceStats[]> {
        const url = endpoints.databases.index.indexesPerformance;
        return this.query<Raven.Client.Data.Indexes.IndexPerformanceStats[]>(url, null, this.db);
    }
}

export = getIndexesPerformance;
