import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveIntegrationsPostgreSqlCredentialsCommand extends commandBase {

    constructor(private db: database, private username: string, private password: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUser;
        const payload = {
            Username: this.username,
            Password: this.password
        }
        
        return this.put<void>(url, JSON.stringify(payload), this.db)
            .done(() => this.reportSuccess("Credentials were saved successfully"))
            .fail((response: JQueryXHR) => this.reportError("Failed to save credentials", response.responseText, response.statusText));
    }
}

export = saveIntegrationsPostgreSqlCredentialsCommand;
