import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class enableIndexCommand extends commandBase {

    private indexName: string;

    private db: database;

    private location: databaseLocationSpecifier;

    constructor(indexName: string, db: database, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            ...this.location
            //TODO: clusterWide: this.clusterWide - how is it related to shards?
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesEnable + this.urlEncodeArgs(args);
        
        //TODO: report messages
        return this.post(url, null, this.db, { dataType: undefined });
    }
}

export = enableIndexCommand; 
