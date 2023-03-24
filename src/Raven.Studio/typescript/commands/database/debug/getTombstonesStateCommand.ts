import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getTombstonesStateCommand extends commandBase {

    constructor(private db: database, private location: databaseLocationSpecifier) {
        super();
    }

    execute(): JQueryPromise<TombstonesStateOnWire> {
        const url = endpoints.databases.adminTombstone.adminTombstonesState;
        const args = this.location;
        return this.query<TombstonesStateOnWire>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get tombstones state", response.responseText, response.statusText));
    }
}

export = getTombstonesStateCommand;
