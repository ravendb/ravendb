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

        this.databasesToExclude(dto.ExcludedDatabases);
    }

    toDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration {
        const backupConfigurationDto = super.toDto();

        const dto = backupConfigurationDto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = this.databasesToExclude();
        return dto;
    }

    static empty(databaseName: KnockoutObservable<string>, serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean, isServerWide: boolean): serverWideBackupConfiguration {
        const dto = periodicBackupConfiguration.emptyDto() as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = null;
        return new serverWideBackupConfiguration(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);
    }
}

export = serverWideBackupConfiguration;
