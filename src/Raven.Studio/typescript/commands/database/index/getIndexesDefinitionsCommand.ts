import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesDefinitionsCommand extends commandBase {
    constructor(private db: database, private skip = 0, private take = 256) {
        super();
    }

    execute(): JQueryPromise<Array<Raven.Client.Documents.Indexes.IndexDefinition>> {
        const args = {
          start: this.skip,
          pageSize: this.take
        };
        const url = endpoints.databases.index.indexes;
        const extractor = (response: resultsDto<Raven.Client.Documents.Indexes.IndexDefinition>) => response.Results;
        return this.query(url, args, this.db, extractor);
    }
}

export = getIndexesDefinitionsCommand;
