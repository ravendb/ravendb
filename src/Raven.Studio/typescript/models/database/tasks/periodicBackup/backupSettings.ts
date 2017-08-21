/// <reference path="../../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

abstract class backupSettings {
    enabled = ko.observable<boolean>();

    connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType;
    
    isTestingCredentials = ko.observable<boolean>();
    testConnectionResult = ko.observable<Raven.Server.Web.System.NodeConnectionTestResult>();
    fullErrorDetailsVisible = ko.observable<boolean>(false);
    shortErrorText: KnockoutObservable<string>;

    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.PeriodicBackup.BackupSettings) {
        this.enabled(!dto.Disabled);
        
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

    toDto(): Raven.Client.ServerWide.PeriodicBackup.BackupSettings {
        return {
            Disabled: !this.enabled()
        }
    }
}

export = backupSettings;
