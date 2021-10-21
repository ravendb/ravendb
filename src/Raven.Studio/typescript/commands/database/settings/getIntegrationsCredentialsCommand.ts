import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIntegrationsCredentialsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLUsernamesList> {
        const url = endpoints.databases.postgreSQL.adminIntegrationPostgresqlUsers;

        return this.query<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLUsernamesList>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get credentials", response.responseText, response.statusText));
    }
}

export = getIntegrationsCredentialsCommand;
