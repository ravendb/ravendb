import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getIntegrationsPostgreSqlSupportCommand extends commandBase {

    constructor(private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlServerStatus> {
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlServerStatus;

        return this.query<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlServerStatus>(url, null, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to get PostgreSQL support info", response.responseText, response.statusText));
    }
}

export = getIntegrationsPostgreSqlSupportCommand;
