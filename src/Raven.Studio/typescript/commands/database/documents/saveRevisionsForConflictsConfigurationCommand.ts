import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;

class saveRevisionsForConflictsConfigurationCommand extends commandBase {
    private readonly db: database;
    private readonly revisionsConfiguration: RevisionsCollectionConfiguration;


    constructor(db: database, revisionsConfiguration: RevisionsCollectionConfiguration) {
        super();
        this.db = db;
        this.revisionsConfiguration = revisionsConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.adminRevisions.adminRevisionsConflictsConfig;
        const args = ko.toJSON(this.revisionsConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save revisions for conflicts configuration", response.responseText, response.statusText));

    }
}

export = saveRevisionsForConflictsConfigurationCommand;
