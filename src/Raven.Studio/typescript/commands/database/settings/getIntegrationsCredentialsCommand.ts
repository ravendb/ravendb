import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIntegrationsCredentialsCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames> {
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUsers;

        return this.query<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get credentials", response.responseText, response.statusText));
    }
}

export = getIntegrationsCredentialsCommand;
