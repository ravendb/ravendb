import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIntegrationsPostgreSqlCredentialsCommand extends commandBase {

    constructor(private db: database, private credentials: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser) {
        super();
    }

    execute(): JQueryPromise<void> {
        
        const url = endpoints.databases.postgreSQL.adminIntegrationPostgresqlUser;
        const payload = this.credentials;
        
        return this.put<void>(url, JSON.stringify(payload), this.db)
            .done(() => this.reportSuccess("Credentials were saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save credentials", response.responseText, response.statusText));
    }
}

export = saveIntegrationsPostgreSqlCredentialsCommand;
