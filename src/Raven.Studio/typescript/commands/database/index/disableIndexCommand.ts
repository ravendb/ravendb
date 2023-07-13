import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");

class disableIndexCommand extends commandBase {

    private readonly indexName: string;
    private readonly db: database;
    private readonly location: databaseLocationSpecifier;

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
            //TODO: clusterWide: this.clusterWide
        };
        
        const url = endpoints.databases.adminIndex.adminIndexesDisable + this.urlEncodeArgs(args);
        
        const locationText = genUtils.formatLocation(this.location);

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to disable index ${this.indexName} for ${locationText}`, response.responseText));
    }
}

export = disableIndexCommand; 
