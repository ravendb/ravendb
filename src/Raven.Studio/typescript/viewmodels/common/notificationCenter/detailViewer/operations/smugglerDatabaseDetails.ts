import app = require("durandal/app");
import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");
import generalUtils = require("common/generalUtils");
import genericProgress = require("common/helpers/database/genericProgress");
import delayBackupCommand = require("commands/database/tasks/delayBackupCommand");
import viewHelpers = require("common/helpers/view/viewHelpers");
import copyToClipboard = require("common/copyToClipboard");

type smugglerListItemStatus = "processed" | "skipped" | "processing" | "pending" | "processedWithErrors";

type smugglerListItem = {
    name: string;
    stage: smugglerListItemStatus;
    hasReadCount: boolean;
    readCount: string;
    hasErroredCount: boolean;
    erroredCount: string;
    hasSkippedCount: boolean;
    skippedCount: string;
    processingSpeedText: string;
    isNested: boolean;
}

type uploadListItem = {
    name: string;
    uploadProgress: genericProgress;
}

class smugglerDatabaseDetails extends abstractOperationDetails {

    view = require("views/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails.html");
    
    private sizeFormatter = generalUtils.formatBytesToSize;

    static extractingDataStageName = "Extracting data";
    static ProcessingText = 'Processing';
    
    detailsVisible = ko.observable<boolean>(false);
    tail = ko.observable<boolean>(true);
    
    itemsLastCount: dictionary<number> = {};
    lastProcessingSpeedText = smugglerDatabaseDetails.ProcessingText;
    
    canDelay: KnockoutComputed<boolean>;
    
    exportItems: KnockoutComputed<Array<smugglerListItem>>;
    uploadItems: KnockoutComputed<Array<uploadListItem>>;
    messages: KnockoutComputed<Array<string>>;
    messagesJoined: KnockoutComputed<string>;
    previousProgressMessages: string[];
    processingSpeed: KnockoutComputed<string>;
    
    filesProcessed: KnockoutComputed<File>;
    
    filesCount: KnockoutComputed<Raven.Client.Documents.Smuggler.SmugglerProgressBase.FileCounts>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.bindToCurrentInstance("toggleDetails");

        this.initObservables();
    }

    protected initObservables() {
        super.initObservables();
        
        this.filesCount = ko.pureComputed(() => {
            if (this.op.status() !== "InProgress") {
                return null;
            }

            const status = this.op.progress();
            if (!status) {
                return null;
            }

            if ("Files" in status) {
                const files = (status as Raven.Client.ServerWide.Operations.RestoreProgress).Files;

                if (!files.FileCount) {
                    return null;
                }
                
                return files;
            }
            
            return null;
        });
        
        this.canDelay = ko.pureComputed(() => {
            const completed = this.op.isCompleted();
            const isBackup = this.op.taskType() === "DatabaseBackup";
            const isOneTime = this.op.message().startsWith("Manual backup"); // unfortunately we don't have better way of detection as of now
            
            return !completed && isBackup && !isOneTime;
        });

        this.exportItems = ko.pureComputed(() => {
            if (this.op.status() === "Faulted") {
                return [];
            }

            const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as Raven.Client.Documents.Smuggler.SmugglerProgressBase;
            if (!status) {
                return [];
            }

            const result: smugglerListItem[] = [];
            if ("SnapshotBackup" in status) {
                const backupCount = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).SnapshotBackup;

                // skip it this case means it is not backup progress object or it is backup of non-binary data
                if (!backupCount.Skipped) {
                    result.push(this.mapToExportListItem("Backed up files", backupCount));
                }
            }

            if ("SnapshotRestore" in status) {
                const restoreCounts = (status as Raven.Client.ServerWide.Operations.RestoreProgress).SnapshotRestore;
                
                // skip it this case means it is not restore progress object or it is restore of non-binary data 
                if (!restoreCounts.Skipped) {
                    result.push(this.mapToExportListItem("Preparing restore", restoreCounts));
                }
            }
            
            if (this.op.taskType() === "MigrationFromLegacyData") {
                const migrationCounts = (status as Raven.Client.Documents.Smuggler.OfflineMigrationProgress).DataExporter;
                result.push(this.mapToExportListItem(smugglerDatabaseDetails.extractingDataStageName, migrationCounts));
            }

            const isDatabaseMigration = this.op.taskType() === "DatabaseMigration";
            
            if (this.op.taskType() === "CollectionImportFromCsv" || isDatabaseMigration) {
                result.push(this.mapToExportListItem("Documents", status.Documents));
                if (isDatabaseMigration) {
                    const attachments = (status.Documents as Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithLastEtagAndAttachments).Attachments;
                    result.push(this.mapToExportListItem("Attachments", attachments, true));
                }
            } else {
                result.push(this.mapToExportListItem("Database Record", status.DatabaseRecord));
                result.push(this.mapToExportListItem("Documents", status.Documents));

                const attachments = (status.Documents as Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithLastEtagAndAttachments).Attachments;
                result.push(this.mapToExportListItem("Attachments", attachments, true));
                
                result.push(this.mapToExportListItem("Counters", status.Counters, true));
                result.push(this.mapToExportListItem("TimeSeries", status.TimeSeries, true));
                
                if (this.op.taskType() === "DatabaseImport") {
                    result.push(this.mapToExportListItem("Tombstones", status.Tombstones, true));
                }
                
                result.push(this.mapToExportListItem("Revisions", status.RevisionDocuments));
                const revisionsAttachments = (status.RevisionDocuments as Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithLastEtagAndAttachments).Attachments;
                result.push(this.mapToExportListItem("Attachments", revisionsAttachments, true));

                result.push(this.mapToExportListItem("Conflicts", status.Conflicts));
                result.push(this.mapToExportListItem("Indexes", status.Indexes));
                result.push(this.mapToExportListItem("Identities", status.Identities));
                result.push(this.mapToExportListItem("Compare Exchange", status.CompareExchange));

                if (this.op.taskType() === "DatabaseImport") {
                    result.push(this.mapToExportListItem("Compare Exchange Tombstones", status.CompareExchangeTombstones, true));
                }
                
                result.push(this.mapToExportListItem("Subscriptions", status.Subscriptions));

                if (status.TimeSeriesDeletedRanges) {
                    result.push(this.mapToExportListItem("Time Series Deleted Ranges", status.TimeSeriesDeletedRanges));
                }
            }

            const currentlyProcessingItems = smugglerDatabaseDetails.findCurrentlyProcessingItems(result);
            
            result.forEach(item => {
                if (item.stage === "processing" && !_.includes(currentlyProcessingItems, item.name)) {
                    item.stage = "pending";
                }

                if (item.stage === "pending" || item.stage === "skipped" || item.name === smugglerDatabaseDetails.extractingDataStageName) {
                    item.hasReadCount = false;
                    item.hasErroredCount = false;
                    item.hasSkippedCount = false;
                    item.readCount = "-";
                    item.erroredCount = "-";
                    item.skippedCount = item.skippedCount || "-";
                }
            });

            return result;
        });

        this.uploadItems = ko.pureComputed(() => {
            if (this.op.taskType() !== "DatabaseBackup") {
                return [];
            }

            if (this.op.status() === "Faulted") {
                return [];
            }

            const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as Raven.Client.Documents.Smuggler.SmugglerProgressBase;
            if (!status) {
                return [];
            }

            const result: uploadListItem[] = [];
            if ("S3Backup" in status) {
                const s3BackupStatus = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).S3Backup;
                const backupStatus = s3BackupStatus as Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
                this.addToUploadItems("S3", result, backupStatus);
            }

            if ("AzureBackup" in status) {
                const azureBackupStatus = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).AzureBackup;
                const backupStatus = azureBackupStatus as Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
                this.addToUploadItems("Azure", result, backupStatus);
            }

            if ("GoogleCloudBackup" in status) {
                const googleCloudBackupStatus = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).GoogleCloudBackup;
                const backupStatus = googleCloudBackupStatus as Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
                this.addToUploadItems("Google Cloud", result, backupStatus);
            }

            if ("GlacierBackup" in status) {
                const glacierBackupStatus = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).GlacierBackup;
                const backupStatus = glacierBackupStatus as Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
                this.addToUploadItems("Glacier", result, backupStatus);
            }

            if ("FtpBackup" in status) {
                const ftpBackupStatus = (status as Raven.Client.Documents.Operations.Backups.BackupProgress).FtpBackup;
                const backupStatus = ftpBackupStatus as Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
                this.addToUploadItems("FTP", result, backupStatus);
            }

            return result;
        });
         
        this.messages = ko.pureComputed(() => {
            if (this.operationFailed()) {
                const errors = this.errorMessages();
                const previousMessages = this.previousProgressMessages || [];
                return previousMessages.concat(...errors);
            } else if (this.op.isCompleted()) {
                const result = this.op.result() as Raven.Client.Documents.Smuggler.SmugglerResult;
                return result ? result.Messages : [];
            } else {
                const progress = this.op.progress() as Raven.Client.Documents.Smuggler.SmugglerResult;
                if (progress) {
                    this.previousProgressMessages = progress.Messages;
                }
                return progress ? progress.Messages : [];
            }
        });

        this.messagesJoined = ko.pureComputed(() => this.messages() ? this.messages().join("\n") : "");
        
        this.registerDisposable(this.operationFailed.subscribe(failed => {
            if (failed) {
                this.detailsVisible(true);
            }
        }));

        if (this.operationFailed()) {
            this.detailsVisible(true);
        }
    }

    delayBackup(duration: string) {
        if (!this.canDelay()) {
            return;
        }
        
        this.close();
        
        const durationFormatted = generalUtils.formatTimeSpan(duration, true);
        
        viewHelpers.confirmationMessage("Delay backup", "Do you want to delay backup by " + durationFormatted +  "?", {
            buttons: ["Cancel", "Delay"]
        })
            .done(result => {
                if (result.can) {
                    new delayBackupCommand(this.op.database, this.op.operationId(), duration)
                        .execute();
                } else {
                    this.openDetails();
                }
            })
            .fail(() => {
                this.openDetails();
            })
        
    }
    
    private static findCurrentlyProcessingItems(result: Array<smugglerListItem>): string[] {
        // since we don't know the contents of smuggler import file we assume that currently processed
        // items are all items with status: 'processing' and positive read count
        // if such item doesn't exist when use first item with processing state
        
        const processing = result.filter(x => x.stage === "processing");
        
        const withPositiveReadCounts = processing.filter(x => x.hasReadCount && x.readCount !== '0');
        if (withPositiveReadCounts.length) {
            return withPositiveReadCounts.map(x => x.name);
        }
        
        if (processing.length) {
            return [processing[0].name];
        }
        
        return [];
    }

    private addToUploadItems(backupType: string, items: Array<uploadListItem>,
        backupStatus: Raven.Client.Documents.Operations.Backups.CloudUploadStatus) {

        if (backupStatus.Skipped) {
            return;
        }

        const uploadProgress = new genericProgress(
            backupStatus.UploadProgress.UploadedInBytes,
            backupStatus.UploadProgress.TotalInBytes,
            (number: number) => this.sizeFormatter(number),
            backupStatus.UploadProgress.BytesPutsPerSec);

        items.push({
            name: `Upload to ${backupType}`,
            uploadProgress: uploadProgress
        });
    }

    private scrollDown() {
        const messages = $(".export-messages")[0];
        if (messages) {
            messages.scrollTop = messages.scrollHeight;
        }
    }

    toggleDetails() {
        this.detailsVisible(!this.detailsVisible());
    }
    
    attached() {
        super.attached();

        this.registerDisposable(this.messagesJoined.subscribe(() => {
            if (this.tail()) {
                this.scrollDown();
            }
        }));

        this.registerDisposable(this.tail.subscribe(enabled => {
            if (enabled) {
                this.scrollDown();
            }
        }));
    }

    private mapToExportListItem(name: string, item: Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts, isNested = false): smugglerListItem {
        let stage: smugglerListItemStatus = "processing";
        
        if (item.Skipped) {
            stage = "skipped";
        } else if (item.Processed) {
            stage = "processed";
            if (item.ErroredCount) {
                stage = "processedWithErrors";
            }
        }

        let processingSpeedText = smugglerDatabaseDetails.ProcessingText;

        const hasSkippedCount = "SkippedCount" in item;
        const skippedCount = hasSkippedCount ? (item as Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithSkippedCountAndLastEtag).SkippedCount : 0;
        
        if (this.showSpeed(name) && item.StartTime) {
            const itemsCount = item.ReadCount + skippedCount + item.ErroredCount;
            
            if (itemsCount === this.itemsLastCount[name]) {
                processingSpeedText = this.lastProcessingSpeedText;
            } else {
                this.itemsLastCount[name] = itemsCount;
                processingSpeedText = this.lastProcessingSpeedText = this.calcSpeedText(itemsCount, item.StartTime);
            }
        }

        return {
            name: name,
            stage: stage,
            hasReadCount: true, // it will be reassigned in post-processing
            readCount: item.ReadCount.toLocaleString(),
            hasSkippedCount: hasSkippedCount,
            skippedCount: hasSkippedCount ? skippedCount.toLocaleString() : "-",
            hasErroredCount: true, // it will be reassigned in post-processing
            erroredCount: item.ErroredCount.toLocaleString(),
            processingSpeedText: processingSpeedText,
            isNested: isNested
        };
    }

    private showSpeed(name: string) {
        return name === "Documents" || name === "Revisions" || name === "Counters" || name === "TimeSeries";
    }
    
    private calcSpeedText(count: number, startTime: string) {
        const durationInSeconds = this.op.getElapsedSeconds(startTime);
        const processingSpeed = abstractOperationDetails.calculateProcessingSpeed(durationInSeconds, count);
        return processingSpeed ? `${processingSpeed.toLocaleString()} items/sec` : smugglerDatabaseDetails.ProcessingText;
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) &&
        (notification.taskType() === "DatabaseExport" ||
            notification.taskType() === "DatabaseImport" ||
            notification.taskType() === "DatabaseMigrationRavenDb" ||
            notification.taskType() === "DatabaseRestore" ||
            notification.taskType() === "MigrationFromLegacyData" ||
            notification.taskType() === "CollectionImportFromCsv" ||
            notification.taskType() === "DatabaseBackup" ||
            notification.taskType() === "DatabaseMigration"
        );
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new smugglerDatabaseDetails(op, center));
    }

    static merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean {
        if (!smugglerDatabaseDetails.supportsDetailsFor(existing)) {
            return false;
        }

        const isUpdate = !_.isUndefined(incoming);

        if (!isUpdate) {
            // object was just created  - only copy message -> message field

            if (!existing.isCompleted()) {
                const result = existing.progress() as Raven.Client.Documents.Smuggler.SmugglerResult;
                result.Messages = [result.Message];
            }
            
        } else if (incoming.State.Status === "InProgress") { // if incoming operation is in progress, then merge messages into existing item
            const incomingResult = incoming.State.Progress as Raven.Client.Documents.Smuggler.SmugglerResult;
            const existingResult = existing.progress() as Raven.Client.Documents.Smuggler.SmugglerResult;

            incomingResult.Messages = existingResult.Messages.concat(incomingResult.Message);
        }

        if (isUpdate) {
            existing.updateWith(incoming);
        }
        
        return true;
    }

    copyLogs() {
        copyToClipboard.copy(this.messagesJoined(), "Copied details to clipboard.");
    }
}

export = smugglerDatabaseDetails;
