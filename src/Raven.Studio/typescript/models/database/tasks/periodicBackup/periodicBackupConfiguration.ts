/// <reference path="../../../../../typings/tsd.d.ts"/>
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import s3Settings = require("models/database/tasks/periodicBackup/s3Settings");
import glacierSettings = require("models/database/tasks/periodicBackup/glacierSettings");
import azureSettings = require("models/database/tasks/periodicBackup/azureSettings");
import ftpSettings = require("models/database/tasks/periodicBackup/ftpSettings");
import getNextBackupOccurrenceCommand = require("commands/database/tasks/getNextBackupOccurrenceCommand");
import getBackupLocationCommand = require("commands/database/tasks/getBackupLocationCommand");
import jsonUtil = require("common/jsonUtil");
import backupSettings = require("backupSettings");
import generalUtils = require("common/generalUtils");
import database = require("models/resources/database");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class periodicBackupConfiguration {
    taskId = ko.observable<number>();
    disabled = ko.observable<boolean>();
    name = ko.observable<string>();
    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();
    fullBackupFrequency = ko.observable<string>();
    incrementalBackupFrequency = ko.observable<string>();
    localSettings = ko.observable<localSettings>();
    s3Settings = ko.observable<s3Settings>();
    glacierSettings = ko.observable<glacierSettings>();
    azureSettings = ko.observable<azureSettings>();
    ftpSettings = ko.observable<ftpSettings>();

    fullBackupHumanReadable: KnockoutComputed<string>;
    fullBackupParsingError = ko.observable<string>();
    nextFullBackupOccurrenceServerTime = ko.observable<string>("N/A");
    nextFullBackupOccurrenceLocalTime = ko.observable<string>();
    nextFullBackupInterval = ko.observable<string>();
    canDisplayNextFullBackupOccurrenceLocalTime: KnockoutComputed<boolean>; 

    incrementalBackupHumanReadable: KnockoutComputed<string>;
    incrementalBackupParsingError = ko.observable<string>();
    nextIncrementalBackupOccurrenceServerTime = ko.observable<string>("N/A");
    nextIncrementalBackupOccurrenceLocalTime = ko.observable<string>();
    nextIncrementalBackupInterval = ko.observable<string>();
    canDisplayNextIncrementalBackupOccurrenceLocalTime: KnockoutComputed<boolean>; 

    manualChooseMentor = ko.observable<boolean>(false);
    preferredMentor = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    backupOptions = ["Backup", "Snapshot"];

    backupLocationInfo = ko.observableArray<Raven.Server.Web.Studio.SingleNodeDataDirectoryResult>([]);
    spinners = {
        backupLocationInfoLoading: ko.observable<boolean>(false)
    };

    dirtyFlag: () => DirtyFlag;

    allBackupFrequencyOptions = [
        { label: "At 02:00 AM (every day)", value: "0 2 * * *", full: true, incremental: false },
        { label: "Every 3 days at 03:00 AM", value: "0 3 */3 * *", full: true, incremental: false },
        { label: "At 08:05 on Sunday (every week)", value: "5 8 * * Sun", full: true, incremental: false },
        { label: "At 11:00 PM, Monday through Friday", value: "0 23 ? * MON-FRI", full: true, incremental: false },
        { label: "At 03:30 AM, on the last day of the month", value: "30 3 L * ?", full: true, incremental: false },
        { label: "Every 10 minutes", value: "*/10 * * * *", full: false, incremental: true },
        { label: "Every 6 hours", value: "0 */6 * * *", full: false, incremental: true },
        { label: "At 15 minutes past the hour, every 3 hours", value: "15 */3 * * *", full: false, incremental: true }
    ];

    constructor(dto: Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration, serverLimits: periodicBackupServerLimitsResponse) {
        this.taskId(dto.TaskId);
        this.disabled(dto.Disabled);
        this.name(dto.Name);
        this.backupType(dto.BackupType);
        this.fullBackupFrequency(dto.FullBackupFrequency);
        this.incrementalBackupFrequency(dto.IncrementalBackupFrequency);
        this.localSettings(!dto.LocalSettings ? localSettings.empty() : new localSettings(dto.LocalSettings));
        this.s3Settings(!dto.S3Settings ? s3Settings.empty(serverLimits.AllowedAwsRegions) : new s3Settings(dto.S3Settings, serverLimits.AllowedAwsRegions));
        this.glacierSettings(!dto.GlacierSettings ? glacierSettings.empty(serverLimits.AllowedAwsRegions) : new glacierSettings(dto.GlacierSettings, serverLimits.AllowedAwsRegions));
        this.azureSettings(!dto.AzureSettings ? azureSettings.empty() : new azureSettings(dto.AzureSettings));
        this.ftpSettings(!dto.FtpSettings ? ftpSettings.empty() : new ftpSettings(dto.FtpSettings));
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);

        this.updateBackupLocationInfo(this.localSettings().folderPath());

        this.initObservables();
    }

    private initObservables() {
        this.fullBackupHumanReadable = ko.pureComputed(() => {
            return periodicBackupConfiguration.getHumanReadable(
                this.fullBackupFrequency,
                this.fullBackupParsingError);
        });

        this.incrementalBackupHumanReadable = ko.pureComputed(() => {
            return periodicBackupConfiguration.getHumanReadable(
                this.incrementalBackupFrequency,
                this.incrementalBackupParsingError);
        });

        this.fullBackupFrequency.throttle(500).subscribe((newValue) =>
            this.getNextOccurance(
                newValue,
                this.nextFullBackupOccurrenceServerTime,
                this.nextFullBackupOccurrenceLocalTime,
                this.nextFullBackupInterval,
                this.fullBackupParsingError));

        this.incrementalBackupFrequency.throttle(500).subscribe((newValue) =>
            this.getNextOccurance(
                newValue,
                this.nextIncrementalBackupOccurrenceServerTime,
                this.nextIncrementalBackupOccurrenceLocalTime,
                this.nextIncrementalBackupInterval,
                this.incrementalBackupParsingError));

        if (this.fullBackupFrequency()) {
            this.getNextOccurance(
                this.fullBackupFrequency(),
                this.nextFullBackupOccurrenceServerTime,
                this.nextFullBackupOccurrenceLocalTime,
                this.nextFullBackupInterval,
                this.fullBackupParsingError);
        }

        if (this.incrementalBackupFrequency()) {
            this.getNextOccurance(
                this.incrementalBackupFrequency(),
                this.nextIncrementalBackupOccurrenceServerTime,
                this.nextIncrementalBackupOccurrenceLocalTime,
                this.nextIncrementalBackupInterval,
                this.incrementalBackupParsingError);
        }

        this.initValidation();

        const anyBackupTypeIsDirty = ko.pureComputed(() => {
            let anyDirty = false;
            const backupTypes = [this.localSettings(), this.s3Settings(), this.glacierSettings(), this.azureSettings(), this.ftpSettings()] as backupSettings[];

            backupTypes.forEach(type => {
                if (type.dirtyFlag().isDirty()) {
                    anyDirty = true;
                }
            });

            return anyDirty;
        });

        this.canDisplayNextFullBackupOccurrenceLocalTime = ko.pureComputed(() =>
            this.nextFullBackupOccurrenceLocalTime() !== this.nextFullBackupOccurrenceServerTime());

        this.canDisplayNextIncrementalBackupOccurrenceLocalTime = ko.pureComputed(() =>
            this.nextIncrementalBackupOccurrenceServerTime() !== this.nextIncrementalBackupOccurrenceLocalTime());

        this.localSettings().folderPath.throttle(300).subscribe((newPathValue) => {
            if (this.localSettings().folderPath.isValid()) {
                this.updateBackupLocationInfo(newPathValue);
            } else {
                this.backupLocationInfo([]);
                this.spinners.backupLocationInfoLoading(false);
            }
        });

        this.dirtyFlag = new ko.DirtyFlag([
            this.name,
            this.backupType,
            this.fullBackupFrequency,
            this.incrementalBackupFrequency,
            this.manualChooseMentor,
            this.preferredMentor,
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

    private static getHumanReadable(backupFrequency: KnockoutObservable<string>,
        backupParsingError: KnockoutObservable<string>): string {
        const currentBackupFrequency = backupFrequency();
        if (!currentBackupFrequency) {
            backupParsingError(null);
            return "N/A";
        }

        const backupFrequencySplitted = currentBackupFrequency.trim().replace(/ +(?= )/g, "").split(" ");
        if (backupFrequencySplitted.length < 5) {
            backupParsingError(`Expression has only ${backupFrequencySplitted.length} part` +
                `${backupFrequencySplitted.length === 1 ? "" : "s"}. ` + 
                "Exactly 5 parts are required!");
            return "N/A";
        } else if (backupFrequencySplitted.length > 5) {
            backupParsingError(`Expression has ${backupFrequencySplitted.length} part` +
                `${backupFrequencySplitted.length === 1 ? "" : "s"}. ` +
                "Exactly 5 parts are required!");
            return "N/A";
        }

        try {
            const result = cronstrue.toString(currentBackupFrequency.toUpperCase());
            if (result.includes("undefined")) {
                backupParsingError("Invalid cron expression!");
                return "N/A";
            }

            backupParsingError(null);
            return result;
        } catch (error) {
            backupParsingError(error);
            return "N/A";
        }
    }

    initValidation() {
        this.backupType.extend({
            required: true
        });

        this.fullBackupFrequency.extend({
            validation: [
                {
                    validator: (fullBackupFrequency: string) =>
                        !(periodicBackupConfiguration.isEmpty(fullBackupFrequency) && periodicBackupConfiguration.isEmpty(this.incrementalBackupFrequency())),
                    message: "Full and incremental backup cannot be both empty"
                },
                {
                    validator: (_: string) => !this.fullBackupParsingError(),
                    message: `{0}`,
                    params: this.fullBackupParsingError
                }
            ]
        });

        this.incrementalBackupFrequency.extend({
            validation: [
                {
                    validator: (incrementalBackupFrequency: string) =>
                        !(periodicBackupConfiguration.isEmpty(incrementalBackupFrequency) && periodicBackupConfiguration.isEmpty(this.fullBackupFrequency())),
                    message: "Full and incremental backup cannot be both empty"
                },
                {
                    validator: (_: string) => !this.incrementalBackupParsingError(),
                    message: `{0}`,
                    params: this.incrementalBackupParsingError
                }
            ]
        });

        this.preferredMentor.extend({
            required: {
                onlyIf: () => this.manualChooseMentor()
            }
        });

        this.validationGroup = ko.validatedObservable({
            backupType: this.backupType,
            fullBackupFrequency: this.fullBackupFrequency,
            incrementalBackupFrequency: this.incrementalBackupFrequency,
            preferredMentor: this.preferredMentor
        });
    }

    private static isEmpty(str: string): boolean {
        if (!str) {
            return true;
        }

        if (!str.trim()) {
            return true;
        }

        return false;
    }

    useBackupType(backupType: Raven.Client.Documents.Operations.Backups.BackupType) {
        this.backupType(backupType);
    }

    createBackupFrequencyAutoCompleter(isFull: boolean) {
        return ko.pureComputed(() => {
            let key = isFull ? this.fullBackupFrequency() : this.incrementalBackupFrequency();

            const options = this.allBackupFrequencyOptions
                .filter(x => isFull ? x.full : x.incremental);

            if (key) {
                key = key.toLowerCase();
                return options.filter(x => x.value.toLowerCase().includes(key));
            } else {
                return options;
            }
        });
    }

    useCronExprssion(isFull: boolean, option: { label: string, value: string }) {
        const selectedOptionObservable = isFull ?
            this.fullBackupFrequency :
            this.incrementalBackupFrequency;
        selectedOptionObservable(option.value);
    }

    getNextOccurance(backupFrequency: string,
        nextBackupOccuranceServerTime: KnockoutObservable<string>,
        nextBackupOccuranceLocalTime: KnockoutObservable<string>,
        nextBackupInterval: KnockoutObservable<string>,
        parsingError: KnockoutObservable<string>) {
        if (parsingError()) {
            nextBackupOccuranceServerTime("N/A");
            nextBackupOccuranceLocalTime("");
            nextBackupInterval("");
            return;
        }

        if (!backupFrequency) {
            nextBackupOccuranceServerTime("N/A");
            nextBackupOccuranceLocalTime("");
            nextBackupInterval("");
            return;
        }

        const dateFormat = generalUtils.dateFormat;
        new getNextBackupOccurrenceCommand(backupFrequency)
            .execute()
            .done((result: Raven.Server.Web.System.NextBackupOccurrence) => {
                const nextBackupServerTime = moment(result.ServerTime).format(dateFormat);
                nextBackupOccuranceServerTime(nextBackupServerTime);
                const nextBackupUtc = moment.utc(result.Utc);
                const nextBackupLocalTime = nextBackupUtc.local().format(dateFormat);
                nextBackupOccuranceLocalTime(nextBackupLocalTime);

                const fromDuration = generalUtils.formatDurationByDate(nextBackupUtc, false);
                nextBackupInterval(`in ${fromDuration}`);

                parsingError(null);
            })
            .fail((response: JQueryXHR) => {
                nextBackupOccuranceServerTime("N/A");
                nextBackupOccuranceLocalTime("");
                nextBackupInterval("");
                parsingError(response.responseText);
            });
    }

    toDto(): Raven.Client.Documents.Operations.Backups.PeriodicBackupConfiguration {
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
            AzureSettings: this.azureSettings().toDto(),
            FtpSettings: this.ftpSettings().toDto(),
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined,
            EncryptionSettings: null
        };
    }

    static empty(serverLimits: periodicBackupServerLimitsResponse): periodicBackupConfiguration {
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
            EncryptionSettings: null
        }, serverLimits);
    }
}

export = periodicBackupConfiguration;
