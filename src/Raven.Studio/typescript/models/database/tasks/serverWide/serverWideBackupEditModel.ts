/// <reference path="../../../../../typings/tsd.d.ts"/>
import periodicBackupConfiguration = require("models/database/tasks/periodicBackup/periodicBackupConfiguration");
import serverWideExcludeModel = require("models/database/tasks/serverWide/serverWideExcludeModel");
import jsonUtil = require("common/jsonUtil");

class serverWideBackupEditModel extends periodicBackupConfiguration {
    excludeInfo = ko.observable<serverWideExcludeModel>();

    serverWideDirtyFlag: () => DirtyFlag;
    serverWideValidationGroup: KnockoutValidationGroup;

    constructor(databaseName: KnockoutObservable<string>,
                dto: Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration,
                serverLimits: periodicBackupServerLimitsResponse,
                encryptedDatabase: boolean,
                isServerWide: boolean) {

        super(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);

        this.excludeInfo(new serverWideExcludeModel(dto.ExcludedDatabases));

        this.initObservablesServerWide();
        this.initValidationServerWide();
    }

    private initObservablesServerWide() {

        this.serverWideDirtyFlag = new ko.DirtyFlag([
            this.excludeInfo().dirtyFlag(),
            this.dirtyFlag()
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private initValidationServerWide() {
        this.serverWideValidationGroup = ko.validatedObservable({
            databasesToExclude: this.excludeInfo().databasesToExclude
        });
    }

    toDto(): Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration {
        const backupConfigurationDto = super.toDto();

        const dto = backupConfigurationDto as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = this.excludeInfo().toDto();
        return dto;
    }

    static empty(databaseName: KnockoutObservable<string>, serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean, isServerWide: boolean): serverWideBackupEditModel {
        const dto = periodicBackupConfiguration.emptyDto() as Raven.Client.ServerWide.Operations.Configuration.ServerWideBackupConfiguration;
        dto.ExcludedDatabases = null;
        return new serverWideBackupEditModel(databaseName, dto, serverLimits, encryptedDatabase, isServerWide);
    }
}

export = serverWideBackupEditModel;
