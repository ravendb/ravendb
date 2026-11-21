import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getTombstonesStateCommand extends commandBase {

    constructor(private readonly db: database | string, private readonly location: databaseLocationSpecifier, private readonly exact = false) {
        super();
    }

    execute(): JQueryPromise<TombstonesStateOnWire> {
        const args = {
            ...this.location,
            exact: this.exact ? true : undefined,
        };
        const url = endpoints.databases.adminTombstone.adminTombstonesState + this.urlEncodeArgs(args);
        return this.query<TombstonesStateOnWire>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get tombstones state", response.responseText, response.statusText));
    }
}

export = getTombstonesStateCommand;
