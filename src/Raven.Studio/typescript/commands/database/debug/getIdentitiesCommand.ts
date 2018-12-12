import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class getIdentitiesCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<dictionary<number>> {
        const url = endpoints.databases.identityDebug.debugIdentities;
        return this.query<dictionary<number>>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to load identities", response.responseText, response.statusText));
    }
}

export = getIdentitiesCommand;
