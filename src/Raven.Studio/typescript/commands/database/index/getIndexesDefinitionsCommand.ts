import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesDefinitionsCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Client.Indexing.IndexDefinition>> {
        const args = {
          start: this.skip,
          pageSize: this.take
        };
        const url = endpoints.databases.index.indexes;
        return this.query(url, args, this.db);
    }
}

export = getIndexesDefinitionsCommand;
