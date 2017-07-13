/// <reference path="../../../../../typings/tsd.d.ts"/>
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import s3Settings = require("models/database/tasks/periodicBackup/s3Settings");
import glacierSettings = require("models/database/tasks/periodicBackup/glacierSettings");
import azureSettings = require("models/database/tasks/periodicBackup/azureSettings");

class periodicBackupConfiguration {
    taskId = ko.observable<number>();
    disabled = ko.observable<boolean>();
    name = ko.observable<string>();
    backupType = ko.observable<Raven.Client.Server.PeriodicBackup.BackupType>();
    fullBackupFrequency = ko.observable<string>();
    incrementalBackupFrequency = ko.observable<string>();
    localSettings = ko.observable<localSettings>();
    s3Settings = ko.observable<s3Settings>();
    glacierSettings = ko.observable<glacierSettings>();
    azureSettings = ko.observable<azureSettings>();

    validationGroup: KnockoutValidationGroup;

    backupOptions = ["Backup", "Snapshot"];

    constructor(dto: Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration) {
        this.taskId(dto.TaskId);
        this.disabled(dto.Disabled);
        this.name(dto.Name);
        this.backupType(dto.BackupType);
        this.fullBackupFrequency(dto.FullBackupFrequency);
        this.incrementalBackupFrequency(dto.IncrementalBackupFrequency);
        this.localSettings(!dto.LocalSettings ? localSettings.empty() : new localSettings(dto.LocalSettings));
        this.s3Settings(!dto.S3Settings ? s3Settings.empty() : new s3Settings(dto.S3Settings));
        this.glacierSettings(!dto.GlacierSettings ? glacierSettings.empty() : new glacierSettings(dto.GlacierSettings));
        this.azureSettings(!dto.AzureSettings ? azureSettings.empty() : new azureSettings(dto.AzureSettings));

        this.initValidation();
    }

    initValidation() {
        this.backupType.extend({
            required: true
        });

        this.fullBackupFrequency.extend({
            validation: [
                {
                    validator: (fullBackupFrequency: string) => this.areBothEmpty(fullBackupFrequency, this.incrementalBackupFrequency()) === false,
                    message: "Full and incremental backup cannot be both empty"
                },
                {
                    validator: (fullBackupFrequency: string) => (this.isEmpty(this.incrementalBackupFrequency()) && this.validateCronExpressionFormat(fullBackupFrequency)) === false,
                    message: "Wrong cron expression format"
                }
            ]
        });

        this.incrementalBackupFrequency.extend({
            validation: [
                {
                    validator: (incrementalBackupFrequency: string) => this.areBothEmpty(incrementalBackupFrequency, this.fullBackupFrequency()) === false,
                    message: "Full and incremental backup cannot be both empty"
                },
                {
                    validator: (incrementalBackupFrequency: string) => (this.isEmpty(this.fullBackupFrequency()) && this.validateCronExpressionFormat(incrementalBackupFrequency)) === false,
                    message: "Wrong cron expression format"
                }
            ]
        });

        this.validationGroup = ko.validatedObservable({
            backupType: this.backupType,
            fullBackupFrequency: this.fullBackupFrequency,
            incrementalBackupFrequency: this.incrementalBackupFrequency
        });
    }

    areBothEmpty(str1: string, str2: string): boolean {
        return this.isEmpty(str1) && this.isEmpty(str2);
    }

    isEmpty(str: string): boolean {
        if (!str) {
            return true;
        }

        if (!str.trim()) {
            return true;
        }

        return false;
    }

    validateCronExpressionFormat(cronExpression: string): boolean {

        return false;
    }

    useBackupType(backupType: Raven.Client.Server.PeriodicBackup.BackupType) {
        this.backupType(backupType);
    }

    toDto(): Raven.Client.Server.PeriodicBackup.PeriodicBackupConfiguration {
        return {
            TaskId: this.taskId(),
            Disabled: this.disabled(),
            Name: this.name(),
            BackupType: this.backupType(),
            FullBackupFrequency: this.fullBackupFrequency(),
            IncrementalBackupFrequency: this.incrementalBackupFrequency(),
            LocalSettings: this.localSettings().toDto(),
            S3Settings: this.s3Settings().toDto(),
            GlacierSettings: this.glacierSettings().toDto(),
            AzureSettings: this.azureSettings().toDto()
        };
    }

    static empty(): periodicBackupConfiguration {
        return new periodicBackupConfiguration({
            TaskId: 0,
            Disabled: false,
            Name: null,
            BackupType: null,
            FullBackupFrequency: null,
            IncrementalBackupFrequency: null,
            LocalSettings: localSettings.empty().toDto(),
            S3Settings: s3Settings.empty().toDto(),
            GlacierSettings: glacierSettings.empty().toDto(),
            AzureSettings: azureSettings.empty().toDto()
        });
    }
}

export = periodicBackupConfiguration;
