import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
type RestoreBackupConfigurationBase = Raven.Client.Documents.Operations.Backups.RestoreBackupConfigurationBase;
type S3Settings = Raven.Client.Documents.Operations.Backups.S3Settings;
type AzureSettings = Raven.Client.Documents.Operations.Backups.AzureSettings;
type GoogleCloudSettings = Raven.Client.Documents.Operations.Backups.GoogleCloudSettings;
type RestoreType = Raven.Client.Documents.Operations.Backups.RestoreType;

export type CreateDatabaseFromBackupDto = Partial<RestoreBackupConfigurationBase> & {
    Type: RestoreType;
} & {
    BackupLocation?: string;
    Settings?: S3Settings | AzureSettings | GoogleCloudSettings;
};

export class restoreDatabaseFromBackupCommand extends commandBase {
    private dto: CreateDatabaseFromBackupDto;

    constructor(dto: CreateDatabaseFromBackupDto) {
        super();
        this.dto = dto;
    }

    execute(): JQueryPromise<operationIdDto> {
        const url = endpoints.global.adminDatabases.adminRestoreDatabase;
        return this.post(url, JSON.stringify(this.dto))
            .done(() => this.reportSuccess(`Started the restore of database named: ${this.dto.DatabaseName}`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to start the restore of database named: ${this.dto.DatabaseName}`,
                response.responseText, response.statusText));
    }
}
