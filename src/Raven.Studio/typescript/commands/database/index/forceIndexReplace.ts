import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");

class forceIndexReplace extends commandBase {

    private readonly indexName: string;
    private readonly db: database;
    private readonly location: databaseLocationSpecifier;

    constructor(indexName: string, db: database, location: databaseLocationSpecifier) {
        super();
        this.indexName = indexName;
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<void> {
        const args = {
            name: this.indexName,
            ...this.location
        }
        const url = endpoints.databases.index.indexesReplace + this.urlEncodeArgs(args);

        const locationText = genUtils.formatLocation(this.location);

        return this.post(url, null, this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to replace index ${this.indexName} for ${locationText}`, response.responseText, response.statusText));
    }
}

export = forceIndexReplace; 
