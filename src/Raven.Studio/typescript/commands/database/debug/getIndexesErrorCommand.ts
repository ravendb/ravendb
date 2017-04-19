import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class GetIndexesErrorCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const url = endpoints.databases.index.indexesErrors;
        return this.query<Raven.Client.Documents.Indexes.IndexErrors[]>(url, null, this.db);
    }
}

export = GetIndexesErrorCommand;
