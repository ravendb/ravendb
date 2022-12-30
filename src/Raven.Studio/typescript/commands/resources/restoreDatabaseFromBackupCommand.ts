import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class restoreDatabaseFromBackupCommand extends commandBase {

    private restoreConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;

    constructor(restoreConfiguration: Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase) {
        super();
        this.restoreConfiguration = restoreConfiguration;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.global.adminDatabases.adminRestoreDatabase;
        return this.post(url, JSON.stringify(this.restoreConfiguration))
            .done(() => this.reportSuccess(`Started the restore of database named: ${this.restoreConfiguration.DatabaseName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to start the restore of database named: ${this.restoreConfiguration.DatabaseName}`,
                response.responseText, response.statusText));
    }
}

export = restoreDatabaseFromBackupCommand; 
