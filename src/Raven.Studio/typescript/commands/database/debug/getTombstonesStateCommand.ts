import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getTombstonesStateCommand extends commandBase {

    private readonly db: database;
    private readonly location: databaseLocationSpecifier;

    constructor(db: database, location: databaseLocationSpecifier) {
        super();
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<TombstonesStateOnWire> {
        const url = endpoints.databases.adminTombstone.adminTombstonesState;
        const args = this.location;
        return this.query<TombstonesStateOnWire>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get tombstones state", response.responseText, response.statusText));
    }
}

export = getTombstonesStateCommand;
