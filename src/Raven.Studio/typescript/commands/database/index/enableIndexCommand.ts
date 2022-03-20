import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class enableIndexCommand extends commandBase {

    constructor(private indexName: string, private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            ...this.location
            //TODO: clusterWide: this.clusterWide - how is it related to shards?
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesEnable + this.urlEncodeArgs(args);
        
        return this.post(url, null, this.db, { dataType: undefined })
            .done(() => {
                const location = ""; //TODO this.clusterWide ? "cluster wide" : "on local node";
                this.reportSuccess(`${this.indexName} was Enabled ${location}`);
             })
            .fail((response: JQueryXHR) => this.reportError("Failed to enable index", response.responseText));
    }
}

export = enableIndexCommand; 
