/// <reference path="../../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

abstract class backupSettings {
    enabled = ko.observable<boolean>();

    connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType;
    
    isTestingCredentials = ko.observable<boolean>();
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    validationGroup: KnockoutValidationGroup;
    
    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.Backups.BackupSettings, connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupConnectionType) {
        this.enabled(!dto.Disabled);
        this.connectionType = connectionType;
        this.initObservables();
    }

    private initObservables() {
        this.shortErrorText = ko.pureComputed(() => {
            const result = this.testConnectionResult();
            if (!result || result.Success) {
                return "";
            }
            return generalUtils.trimMessage(result.Error);
        });
    }

    validate(action: () => boolean): boolean {
        if (!this.enabled())
            return true;

        return action();
    }

    toDto(): Raven.Client.Documents.Operations.Backups.BackupSettings {
        return {
            Disabled: !this.enabled(),
            GetBackupConfigurationScript: null
        }
    }
}

export = backupSettings;
