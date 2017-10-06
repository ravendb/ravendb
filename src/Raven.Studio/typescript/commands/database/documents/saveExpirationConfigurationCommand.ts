import database = require("models/resources/database");
import commandBase = require("commands/commandBase");
import endpoint = require("endpoints");

class saveExpirationConfigurationCommand extends commandBase {
    constructor(private db: database, private expirationConfiguration: Raven.Client.ServerWide.Expiration.ExpirationConfiguration) {
        super();
    }

    execute(): JQueryPromise<updateDatabaseConfigurationsResult> {

        const url = endpoint.global.adminDatabases.adminExpirationConfig + this.urlEncodeArgs({ name: this.db.name });
        const args = ko.toJSON(this.expirationConfiguration);
        return this.post<updateDatabaseConfigurationsResult>(url, args)
            .fail((response: JQueryXHR) => this.reportError("Failed to save expiration configuration", response.responseText, response.statusText));

    }
}

export = saveExpirationConfigurationCommand;
