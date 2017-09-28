import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesStatusCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexingStatus> {
        const url = endpoints.databases.index.indexesStatus;
        return this.query<Raven.Client.Documents.Indexes.IndexingStatus>(url, null, this.db)
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get indexing status!", response.responseText, response.statusText);
            });
    }
}

export = getIndexesStatusCommand;
