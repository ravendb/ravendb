import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveRevisionsConfigurationCommand extends commandBase {
    constructor(private db: database, private revisionsConfiguration: Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.adminRevisions.adminRevisionsConfig;
        const args = ko.toJSON(this.revisionsConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save revisions configuration", response.responseText, response.statusText));

    }
}

export = saveRevisionsConfigurationCommand;
