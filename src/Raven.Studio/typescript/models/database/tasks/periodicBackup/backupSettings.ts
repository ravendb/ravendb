/// <reference path="../../../../../typings/tsd.d.ts"/>

abstract class backupSettings {
    enabled = ko.observable<boolean>();

    connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType;
    isTestingCredentials = ko.observable<boolean>();
    validationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.ServerWide.PeriodicBackup.BackupSettings) {
        this.enabled(!dto.Disabled);
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