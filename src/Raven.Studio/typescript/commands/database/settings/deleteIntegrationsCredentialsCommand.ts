import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteIntegrationsCredentialsCommand extends commandBase {

    constructor(private db: database, private username: string) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            username: this.username
        };
        
        const url = endpoints.databases.postgreSqlIntegration.adminIntegrationsPostgresqlUser + this.urlEncodeArgs(args);

        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully deleted credentials for user - ${this.username}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete credentials for user - ${this.username}`, response.responseText, response.statusText));
    }
}

export = deleteIntegrationsCredentialsCommand;
