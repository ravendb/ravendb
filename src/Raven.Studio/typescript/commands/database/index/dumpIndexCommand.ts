import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class dumpIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database, private dumpDirectoryPath: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            path: this.dumpDirectoryPath
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesDump + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(`Created dump files for index ${this.indexName}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to dump index files", response.responseText));
    }
}

export = dumpIndexCommand; 
