import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteDocsMatchingQueryCommand extends commandBase {
    constructor(private indexName: string, private queryText: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            query: this.queryText,
            allowStale: false
        };

        const url = endpoints.databases.queries.queries + this.indexName + this.urlEncodeArgs(args);
        return this.del(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Error deleting docs matching query", response.responseText, response.statusText));
    }

}

export = deleteDocsMatchingQueryCommand; 
