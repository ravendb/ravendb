import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getConnectionStringsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult> {
        const url = endpoints.databases.ongoingTasks.adminConnectionStrings;

        return this.query<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get connection strings", response.responseText, response.statusText));
    }
}

export = getConnectionStringsCommand;
