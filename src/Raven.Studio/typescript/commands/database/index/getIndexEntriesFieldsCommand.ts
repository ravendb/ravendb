import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexEntriesFieldsCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<resultsDto<string>> {
        return this.getIndexEntries()
            .fail((response: JQueryXHR) => {
                this.reportError("Failed to get index entries", response.responseText, response.statusText);
            });
    }
    
    private getIndexEntries(): JQueryPromise<resultsDto<string>> {
        const args = {
            name: this.indexName,
            op: "entries-fields"
        };
        
        const url = endpoints.databases.index.indexesDebug;

        return this.query(url, args, this.db);
    }
}

export = getIndexEntriesFieldsCommand;
