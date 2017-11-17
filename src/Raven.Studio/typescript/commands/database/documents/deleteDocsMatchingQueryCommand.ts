import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteDocsMatchingQueryCommand extends commandBase {
    constructor(private queryText: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            Query: this.queryText,
            WaitForNonStaleResults: true
        };

        const url = endpoints.databases.queries.queries;
        return this.del<operationIdDto>(url, JSON.stringify(args), this.db)
            .fail((response: JQueryXHR) => this.reportError("Error deleting docs matching query", response.responseText, response.statusText));
    }

}

export = deleteDocsMatchingQueryCommand; 
