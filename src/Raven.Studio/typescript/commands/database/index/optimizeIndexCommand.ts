import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class optimizeIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const args = {
            name: this.indexName,
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesOptimize + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(`Started to optimize index: ${this.indexName}`))
            .fail((response: JQueryXHR) => this.reportError("Failed to optimize index files", response.responseText));
    }
}

export = optimizeIndexCommand; 
