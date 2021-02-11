import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexHistoryCommand extends commandBase {

    constructor(private db: database, private indexName: string) {
        super();
    }

    execute(): JQueryPromise<indexHistoryCommandResult> {
        const args = { name: this.indexName };
        const url = endpoints.databases.index.indexesHistory;
        
        return this.query<indexHistoryCommandResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get index history", response.responseText, response.statusText));
    }
} 

export = getIndexHistoryCommand;
