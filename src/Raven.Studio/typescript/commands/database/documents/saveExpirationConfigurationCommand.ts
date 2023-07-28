import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");
import ExpirationConfiguration = Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration;

class saveExpirationConfigurationCommand extends commandBase {

    private readonly db: database;
    private readonly expirationConfiguration: ExpirationConfiguration

    constructor(db: database, expirationConfiguration: ExpirationConfiguration) {
        super();
        this.db = db;
        this.expirationConfiguration = expirationConfiguration;
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.expiration.adminExpirationConfig;
        const args = ko.toJSON(this.expirationConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save expiration configuration", response.responseText, response.statusText));

    }
}

export = saveExpirationConfigurationCommand;
