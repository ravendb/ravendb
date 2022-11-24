import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getTombstonesStateCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<TombstonesStateOnWire> {
        const url = endpoints.databases.adminTombstone.adminTombstonesState;
        return this.query<TombstonesStateOnWire>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to tombstones state", response.responseText, response.statusText));
    }
}

export = getTombstonesStateCommand;
