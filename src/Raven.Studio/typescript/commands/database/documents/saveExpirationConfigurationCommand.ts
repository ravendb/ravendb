import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveExpirationConfigurationCommand extends commandBase {
    constructor(private db: database, private expirationConfiguration: Raven.Client.Documents.Operations.Expiration.ExpirationConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.databases.expiration.adminExpirationConfig;
        const args = ko.toJSON(this.expirationConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save expiration configuration", response.responseText, response.statusText));

    }
}

export = saveExpirationConfigurationCommand;
