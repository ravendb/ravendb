/// <reference path="../../../../typings/tsd.d.ts"/>
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import generalUtils = require("common/generalUtils");

class manualBackupListModel {

    taskId = 0;
    backupType = ko.observable<string>();
    isEncrypted = ko.observable<boolean>();
    nodeTag = ko.observable<string>();

    lastFullBackup = ko.observable<string>();
    lastFullBackupHumanized: KnockoutComputed<string>;
    backupDestinationsHumanized: KnockoutComputed<string>;

    activeDatabase = activeDatabaseTracker.default.database;
    
    constructor(dto: Raven.Client.Documents.Operations.Backups.PeriodicBackupStatus) {

        this.backupType(dto.BackupType);
        this.isEncrypted(dto.IsEncrypted);
        this.nodeTag(dto.NodeTag);

        this.lastFullBackup(dto.LastFullBackup);
        this.lastFullBackupHumanized = ko.pureComputed(() => {
            const lastFullBackup = dto.LastFullBackup;
            if (!lastFullBackup) {
                return "Never backed up";
            }

            return generalUtils.formatDurationByDate(moment.utc(lastFullBackup), true);
        });

        this.backupDestinationsHumanized = ko.pureComputed(() => {
            let destinations: Array<string> = [];

            if (dto.LocalBackup && dto.LocalBackup.BackupDirectory) {
                destinations.push("Local");
            }
            if (!dto.UploadToS3.Skipped) {
                destinations.push("S3");
            }
            if (!dto.UploadToGlacier.Skipped) {
                destinations.push("Glacier");
            }
            if (!dto.UploadToGlacier.Skipped) {
                destinations.push("Glacier");
            }
            if (!dto.UploadToAzure.Skipped) {
                destinations.push("Azure");
            }
            if (!dto.UploadToGoogleCloud.Skipped) {
                destinations.push("Google Cloud");
            }
            if (!dto.UploadToFtp.Skipped) {
                destinations.push("Ftp");
            }

            return destinations.length ? destinations.join(', ') : "No destinations defined";
        });
    }
}

export = manualBackupListModel;
