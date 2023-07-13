import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");
import genUtils = require("common/generalUtils");

class resetIndexCommand extends commandBase {

    private readonly indexName: string;
    private readonly db: database;
    private readonly location: databaseLocationSpecifier;

    constructor(indexName: string, db: database, location: databaseLocationSpecifier) {
        super();
        this.location = location;
        this.db = db;
        this.indexName = indexName;
    }

    execute(): JQueryPromise<{ IndexId: number }> {
        const args = {
            name: this.indexName, 
            ...this.location
        };
        const url = endpoints.databases.index.indexes + this.urlEncodeArgs(args);

        const locationText = genUtils.formatLocation(this.location);

        return this.reset<{ IndexId: number }>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError(`Failed to reset index ${this.indexName} for ${locationText}`, response.responseText, response.statusText));
    }
}

export = resetIndexCommand;
