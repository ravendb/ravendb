/// <reference path="../../../../../typings/tsd.d.ts"/>
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import s3Settings = require("models/database/tasks/periodicBackup/s3Settings");
import glacierSettings = require("models/database/tasks/periodicBackup/glacierSettings");
import azureSettings = require("models/database/tasks/periodicBackup/azureSettings");
import ftpSettings = require("models/database/tasks/periodicBackup/ftpSettings");
import getBackupLocationCommand = require("commands/database/tasks/getBackupLocationCommand");
import getFolderPathOptionsCommand = require("commands/resources/getFolderPathOptionsCommand");
import jsonUtil = require("common/jsonUtil");
import backupSettings = require("backupSettings");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import encryptionSettings = require("models/database/tasks/periodicBackup/encryptionSettings");

class periodicBackupConfiguration {
    
    static readonly defaultFullBackupFrequency = "0 2 * * 0";
    static readonly defaultIncrementalBackupFrequency = "0 2 * * *";
    
    taskId = ko.observable<number>();
    disabled = ko.observable<boolean>();
    name = ko.observable<string>();
    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();

    fullBackupEnabled = ko.observable<boolean>(false);
    fullBackupFrequency = ko.observable<string>();
    
    incrementalBackupEnabled = ko.observable<boolean>(false);
    incrementalBackupFrequency = ko.observable<string>();
    
    localSettings = ko.observable<localSettings>();
    s3Settings = ko.observable<s3Settings>();
    glacierSettings = ko.observable<glacierSettings>();
    azureSettings = ko.observable<azureSettings>();
    ftpSettings = ko.observable<ftpSettings>();
    encryptionSettings = ko.observable<encryptionSettings>();
    
    manualChooseMentor = ko.observable<boolean>(false);
    mentorNode = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    backupOptions = ["Backup", "Snapshot"];

    backupLocationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    folderPathOptions = ko.observableArray<string>([]);

    spinners = {
        backupLocationInfoLoading: ko.observable<boolean>(false)
    };

    dirtyFlag: () => DirtyFlag;

    constructor(dto: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration, serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean) {
        this.taskId(dto.TaskId);
        this.disabled(dto.Disabled);
        this.name(dto.Name);
        this.backupType(dto.BackupType);
        this.fullBackupEnabled(!!dto.FullBackupFrequency);
        this.fullBackupFrequency(dto.FullBackupFrequency || periodicBackupConfiguration.defaultFullBackupFrequency);
        this.incrementalBackupEnabled(!!dto.IncrementalBackupFrequency);
        this.incrementalBackupFrequency(dto.IncrementalBackupFrequency || periodicBackupConfiguration.defaultIncrementalBackupFrequency);
        this.localSettings(!dto.LocalSettings ? localSettings.empty() : new localSettings(dto.LocalSettings));
        this.s3Settings(!dto.S3Settings ? s3Settings.empty(serverLimits.AllowedAwsRegions) : new s3Settings(dto.S3Settings, serverLimits.AllowedAwsRegions));
        this.glacierSettings(!dto.GlacierSettings ? glacierSettings.empty(serverLimits.AllowedAwsRegions) : new glacierSettings(dto.GlacierSettings, serverLimits.AllowedAwsRegions));
        this.azureSettings(!dto.AzureSettings ? azureSettings.empty() : new azureSettings(dto.AzureSettings));
        this.ftpSettings(!dto.FtpSettings ? ftpSettings.empty() : new ftpSettings(dto.FtpSettings));
        
        this.manualChooseMentor(!!dto.MentorNode);
        this.mentorNode(dto.MentorNode);

        const folderPath = this.localSettings().folderPath();
        if (folderPath) {
            this.updateBackupLocationInfo(folderPath);
        }

        this.updateFolderPathOptions(folderPath);

        this.encryptionSettings(new encryptionSettings(encryptedDatabase, this.backupType, dto.BackupEncryptionSettings));

        this.initValidation();

        const anyBackupTypeIsDirty = ko.pureComputed(() => {
            let anyDirty = false;
            const backupTypes = [this.localSettings(), this.s3Settings(), this.glacierSettings(), this.azureSettings(), this.ftpSettings()] as backupSettings[];

            backupTypes.forEach(type => {
                if (type.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });
            
            if (this.encryptionSettings().dirtyFlag().isDirty()) {
                anyDirty = true;
            }
            
            return anyDirty;
        });
        
        this.localSettings().folderPath.throttle(300).subscribe((newPathValue) => {
            if (this.localSettings().folderPath.isValid()) {
                this.updateBackupLocationInfo(newPathValue);
                this.updateFolderPathOptions(newPathValue);
            } else {
                this.backupLocationInfo([]);
                this.folderPathOptions([]);
                this.spinners.backupLocationInfoLoading(false);
            }
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.backupType,
            this.fullBackupFrequency,
            this.fullBackupEnabled,
            this.incrementalBackupFrequency,
            this.incrementalBackupEnabled,
            this.manualChooseMentor,
            this.mentorNode,
            anyBackupTypeIsDirty
        ], false, jsonUtil.newLineNormalizingHashFunction);
    }

    private updateBackupLocationInfo(path: string) {
        this.spinners.backupLocationInfoLoading(true);

        new getBackupLocationCommand(path, activeDatabaseTracker.default.database())
            .execute()
            .done((result: Raven.Server.Web.Studio.DataDirectoryResult) => {
                if (!this.spinners.backupLocationInfoLoading()) {
                    return;
                }

                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.backupLocationInfo(result.List);
            })
            .always(() => this.spinners.backupLocationInfoLoading(false));
    }

    private updateFolderPathOptions(path: string) {
        new getFolderPathOptionsCommand(path, true)
            .execute()
            .done((result: Raven.Server.Web.Studio.FolderPathOptions) => {
                if (this.localSettings().folderPath() !== path) {
                    // the path has changed
                    return;
                }

                this.folderPathOptions(result.List);
            });
    }

    initValidation() {
        this.backupType.extend({
            required: true
        });

        this.incrementalBackupEnabled.extend({
            validation: [
                {
                    validator: (e: boolean) => this.fullBackupEnabled() || e,
                    message: "Please select either full or incremental backup"
                }
            ]
        });
        
        this.fullBackupFrequency.extend({
            required: {
                onlyIf: () => this.fullBackupEnabled()
            }
        });
        
        this.incrementalBackupFrequency.extend({
            required: {
                onlyIf: () => this.incrementalBackupEnabled()
            }
        });
        
        this.mentorNode.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });
        
        this.validationGroup = ko.validatedObservable({
            backupType: this.backupType,
            fullBackupFrequency: this.fullBackupFrequency,
            fullBackupEnabled: this.fullBackupEnabled,
            incrementalBackupFrequency: this.incrementalBackupFrequency,
            incrementalBackupEnabled: this.incrementalBackupEnabled,
            mentorNode: this.mentorNode
        });
    }

    private static isEmpty(str: string): boolean {
        if (!str) {
            return true;
        }

        return !str.trim();
    }

    useBackupType(backupType: Raven.Client.Documents.Operations.Backups.BackupType) {
        this.backupType(backupType);
    }

    toDto(): Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration {
        return {
            TaskId: this.taskId(),
            Disabled: this.disabled(),
            Name: this.name(),
            BackupType: this.backupType(),
            FullBackupFrequency: this.fullBackupEnabled() ? this.fullBackupFrequency() : null,
            IncrementalBackupFrequency: this.incrementalBackupEnabled() ? this.incrementalBackupFrequency() : null,
            LocalSettings: this.localSettings().toDto(),
            S3Settings: this.s3Settings().toDto(),
            GlacierSettings: this.glacierSettings().toDto(),
            AzureSettings: this.azureSettings().toDto(),
            FtpSettings: this.ftpSettings().toDto(),
            MentorNode: this.manualChooseMentor() ? this.mentorNode() : undefined,
            BackupEncryptionSettings: this.encryptionSettings().toDto()
        };
    }

    static empty(serverLimits: periodicBackupServerLimitsResponse, encryptedDatabase: boolean): periodicBackupConfiguration {
        return new periodicBackupConfiguration({
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
            FtpSettings: null,
            MentorNode: null,
            BackupEncryptionSettings: {
                Key: "",
                EncryptionMode: null
            }
        }, serverLimits, encryptedDatabase);
    }
}

export = periodicBackupConfiguration;
