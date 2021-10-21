import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIntegrationsCredentialsCommand extends commandBase {

    constructor(private db: database, private credentials: Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSQLNewUser) {
        super();
    }
 
    execute(): JQueryPromise<void> { 
        return this.savePostgreSqlCredentials()
            .fail((response: JQueryXHR) => this.reportError("Failed to save credentials", response.responseText, response.statusText))
            .done(() => this.reportSuccess(`Credentials were saved successfully`));
    }

    private savePostgreSqlCredentials(): JQueryPromise<void> {
        const saveCredentialsTask = $.Deferred<void>();
        
        const url = endpoints.databases.postgreSQL.adminIntegrationPostgresqlUser;
        const payload = this.credentials;

        this.put(url, JSON.stringify(payload), this.db)
            .done(() => saveCredentialsTask.resolve())
            .fail(response => saveCredentialsTask.reject(response));

        return saveCredentialsTask;
    }
}

export = saveIntegrationsCredentialsCommand; 

