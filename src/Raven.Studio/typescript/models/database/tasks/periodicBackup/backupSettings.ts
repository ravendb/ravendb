/// <reference path="../../../../../typings/tsd.d.ts"/>

abstract class backupSettings {
    enabled = ko.observable<boolean>();
    enabledOneTimeValue: boolean;

    connectionType: Raven.Server.Documents.PeriodicBackup.PeriodicBackupTestConnectionType;
    isTestingCredentials = ko.observable<boolean>();

    validationGroup: KnockoutValidationGroup;
    credentialsValidationGroup: KnockoutValidationGroup;

    constructor(dto: Raven.Client.Server.PeriodicBackup.BackupSettings) {
        this.enabled(!dto.Disabled);
        this.enabledOneTimeValue = !dto.Disabled;
    }

    validate(action: () => boolean): boolean {
        if (!this.enabled())
            return true;

        return action();
    }

    toDto(): Raven.Client.Server.PeriodicBackup.BackupSettings {
        return {
            Disabled: !this.enabled()
        }
    }

}

export = backupSettings;