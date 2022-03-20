import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIndexesErrorCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Indexes.IndexErrors[]> {
        const args = { ...this.location };
        const url = endpoints.databases.index.indexesErrors + this.urlEncodeArgs(args);

        const locationText = `Node: ${this.location.nodeTag}` + (this.location.shardNumber !== undefined ? ` and Shard ${this.location.shardNumber}` : '');
        
        return this.query<Raven.Client.Documents.Indexes.IndexErrors[]>(url, null, this.db, x => x.Results)
            .fail((result: JQueryXHR) => this.reportError(`Failed to get index errors for ${locationText}`, result.responseText, result.statusText));
    }
}

export = getIndexesErrorCommand;
