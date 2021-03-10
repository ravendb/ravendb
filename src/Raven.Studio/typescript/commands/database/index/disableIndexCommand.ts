import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class disableIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database, private clusterWide: boolean) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            clusterWide: this.clusterWide
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesDisable + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                const location = this.clusterWide ? "cluster wide" : "on local node";
                this.reportSuccess(`${this.indexName} was Disabled ${location}`);
             })
            .fail((response: JQueryXHR) => this.reportError("Failed to disable index", response.responseText));
    }
}

export = disableIndexCommand; 
