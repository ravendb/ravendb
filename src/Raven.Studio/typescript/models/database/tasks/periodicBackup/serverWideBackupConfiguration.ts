/// <reference path="../../../../../typings/tsd.d.ts"/>
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");

class serverWideBackupConfiguration extends periodicBackupConfiguration {
    
    databasesToExclude = ko.observableArray<string>();

    constructor(databaseName: KnockoutObservable<string>,
                dto: Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration, 
                serverLimits: periodicBackupServerLimitsResponse, 
                encryptedDatabase: boolean,
        isServerWide: boolean) {

        super(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);
    }

    toDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration {
        const backupConfigurationDto = super.toDto();

        const dto = backupConfigurationDto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.DatabasesToExclude = this.databasesToExclude();
        return dto;
    }

    static empty(databaseName: KnockoutObservable<string>, serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean, isServerWide: boolean): serverWideBackupConfiguration {
        return new serverWideBackupConfiguration(databaseName, {
            TaskId: 0,
            Disabled: false,
            Name: null,
            BackupType: null,
            FullBackupFrequency: null,
            IncrementalBackupFrequency: null,
            LocalSettings: null,
            S3Settings: null,
            GlacierSettings: null,
            AzureSettings: null,
            GoogleCloudSettings: null,
            FtpSettings: null,
            MentorNode: null,
            BackupEncryptionSettings: {
                Key: "",
                EncryptionMode: null
            },
            SnapshotSettings: null,
            RetentionPolicy: null,
            ExcludedDatabases: null
        }, serverLimits, encryptedDatabase, isServerWide);
    }
}

export = serverWideBackupConfiguration;
