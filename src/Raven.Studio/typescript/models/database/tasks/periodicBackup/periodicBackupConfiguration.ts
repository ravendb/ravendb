/// <reference path="../../../../../typings/tsd.d.ts"/>
import localSettings = require("models/database/tasks/periodicBackup/localSettings");
import s3Settings = require("models/database/tasks/periodicBackup/s3Settings");
import glacierSettings = require("models/database/tasks/periodicBackup/glacierSettings");
import azureSettings = require("models/database/tasks/periodicBackup/azureSettings");
import ftpSettings = require("models/database/tasks/periodicBackup/ftpSettings");
import getNextBackupOccurrenceCommand = require("commands/database/tasks/getNextBackupOccurrenceCommand");

class periodicBackupConfiguration {
    taskId = ko.observable<number>();
    disabled = ko.observable<boolean>();
    name = ko.observable<string>();
    backupType = ko.observable<Raven.Client.ServerWide.PeriodicBackup.BackupType>();
    fullBackupFrequency = ko.observable<string>();
    incrementalBackupFrequency = ko.observable<string>();
    localSettings = ko.observable<localSettings>();
    s3Settings = ko.observable<s3Settings>();
    glacierSettings = ko.observable<glacierSettings>();
    azureSettings = ko.observable<azureSettings>();
    ftpSettings = ko.observable<ftpSettings>();

    fullBackupHumanReadable: KnockoutComputed<string>;
    fullBackupParsingError = ko.observable<string>();
    nextFullBackupOccurrence = ko.observable<string>("N/A");
    incrementalBackupHumanReadable: KnockoutComputed<string>;
    incrementalBackupParsingError = ko.observable<string>();
    nextIncrementalBackupOccurrence = ko.observable<string>("N/A");

    manualChooseMentor = ko.observable<boolean>(false);
    preferredMentor = ko.observable<string>();

    validationGroup: KnockoutValidationGroup;
    backupOptions = ["Backup", "Snapshot"];

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

    constructor(dto: Raven.Client.ServerWide.PeriodicBackup.PeriodicBackupConfiguration) {
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
        this.ftpSettings(!dto.FtpSettings ? ftpSettings.empty() : new ftpSettings(dto.FtpSettings));

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
                this.nextFullBackupOccurrence,
                this.fullBackupParsingError));

        this.incrementalBackupFrequency.throttle(500).subscribe((newValue) =>
            this.getNextOccurance(
                newValue,
                this.nextIncrementalBackupOccurrence,
                this.incrementalBackupParsingError));

        if (this.fullBackupFrequency()) {
            this.getNextOccurance(
                this.fullBackupFrequency(),
                this.nextFullBackupOccurrence,
                this.fullBackupParsingError);
        }

        if (this.incrementalBackupFrequency()) {
            this.getNextOccurance(
                this.incrementalBackupFrequency(),
                this.nextIncrementalBackupOccurrence,
                this.incrementalBackupParsingError);
        }
        
        this.manualChooseMentor(!!dto.MentorNode);
        this.preferredMentor(dto.MentorNode);

        this.initValidation();
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
        }
        else if (backupFrequencySplitted.length > 5) {
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

    useBackupType(backupType: Raven.Client.ServerWide.PeriodicBackup.BackupType) {
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
        nextBackupOccurance: KnockoutObservable<string>,
        parsingError: KnockoutObservable<string>) {
        if (parsingError())
            return;

        if (!backupFrequency)
            return;

        const now = moment();
        const dateFormat = "YYYY MMMM Do, h:mm A";
        new getNextBackupOccurrenceCommand(now.format(), backupFrequency)
            .execute()
            .done((result: Raven.Server.Web.System.NextBackupOccurrence) => {
                const nextBackup = moment(result.DateTime).format(dateFormat);
                nextBackupOccurance(nextBackup);
                parsingError(null);
            })
            .fail((response: JQueryXHR) => {
                nextBackupOccurance("N/A");
                parsingError(response.responseText);
            });
    }

    toDto(): Raven.Client.ServerWide.PeriodicBackup.PeriodicBackupConfiguration {
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
            MentorNode: this.manualChooseMentor() ? this.preferredMentor() : undefined
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
            AzureSettings: azureSettings.empty().toDto(),
            FtpSettings: ftpSettings.empty().toDto(),
            MentorNode: null
        });
    }
}

export = periodicBackupConfiguration;
