import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ConnectionStrings.GetConnectionStringsResult> {
        const args = { name: this.db.name };
        const url = endpoints.global.adminDatabases.adminConnectionStrings;

        return this.query<Raven.Client.ServerWide.Operations.ConnectionStrings.GetConnectionStringsResult>(url, args)
            .fail((response: JQueryXHR) => this.reportError("Failed to get connection strings", response.responseText, response.statusText));
    }
}

export = getConnectionStringsCommand;