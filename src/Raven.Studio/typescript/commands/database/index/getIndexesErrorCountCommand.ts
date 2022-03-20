import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesErrorCountCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }
    
    execute(): JQueryPromise<{Results: indexErrorsCount[]}> {
        const args = { ...this.location };
        const url = endpoints.databases.studioIndex.studioIndexesErrorsCount;

        const locationText = `Node: ${this.location.nodeTag}` + (this.location.shardNumber !== undefined ? ` and Shard ${this.location.shardNumber}` : '');
            
        return this.query<any>(url, args, this.db)
            .fail((result: JQueryXHR) => this.reportError(`Failed to get index errors count for ${locationText}`,
                result.responseText, result.statusText));
    }
}

export = getIndexesErrorCountCommand;
