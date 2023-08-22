import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;

class saveRevisionsConfigurationCommand extends commandBase {
    private readonly db: database;
    private readonly revisionsConfiguration: RevisionsConfiguration;

    constructor(db: database, revisionsConfiguration: RevisionsConfiguration) {
        super();
        this.db = db;
        this.revisionsConfiguration = revisionsConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.adminRevisions.adminRevisionsConfig;
        const args = ko.toJSON(this.revisionsConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save revisions configuration", response.responseText, response.statusText));

    }
}

export = saveRevisionsConfigurationCommand;
