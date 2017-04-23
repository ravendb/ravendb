import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesErrorCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const url = endpoints.databases.index.indexesErrors;
        return this.query<Raven.Client.Documents.Indexes.IndexErrors[]>(url, null, this.db)
            .fail((result: JQueryXHR) => this.reportError("Failed to get index errors", result.responseText, result.statusText));
    }
}

export = getIndexesErrorCommand;
