import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

class restoreDatabaseFromBackupCommand extends commandBase {

    constructor(private restoreConfiguration: Raven.Client.Server.PeriodicBackup.RestoreBackupConfiguration) {
        super();
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.global.adminDatabases.adminDatabaseRestore;
        return this.post(url, JSON.stringify(this.restoreConfiguration))
            .done(() => this.reportSuccess(`Started the restore of database named: ${this.restoreConfiguration.DatabaseName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to start the restore of database named: ${this.restoreConfiguration.DatabaseName}`,
                response.responseText, response.statusText));
    }
}

export = restoreDatabaseFromBackupCommand; 
