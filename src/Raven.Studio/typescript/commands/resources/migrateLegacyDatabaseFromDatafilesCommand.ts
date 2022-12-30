import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class migrateLegacyDatabaseFromDatafilesCommand extends commandBase {

    private restoreConfiguration: Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration;

    constructor(restoreConfiguration: Raven.Client.ServerWide.Operations.Migration.OfflineMigrationConfiguration) {
        super();
        this.restoreConfiguration = restoreConfiguration;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.global.adminDatabases.adminMigrateOffline;
        return this.post(url, JSON.stringify(this.restoreConfiguration))
            .done(() => this.reportSuccess(`Started migration of database named: ${this.restoreConfiguration.DatabaseRecord.DatabaseName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to start migration of database named: ${this.restoreConfiguration.DatabaseRecord.DatabaseName}`,
                response.responseText, response.statusText));
    }
}

export = migrateLegacyDatabaseFromDatafilesCommand; 
