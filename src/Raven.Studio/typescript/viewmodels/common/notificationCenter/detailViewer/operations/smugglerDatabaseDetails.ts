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
import ShardedSmugglerResult = Raven.Client.Documents.Smuggler.ShardedSmugglerResult;
import CountsWithSkippedCountAndLastEtag = Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithSkippedCountAndLastEtag;
import CountsWithSkippedCountAndLastEtagAndAttachments = Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithSkippedCountAndLastEtagAndAttachments;
import Counts = Raven.Client.Documents.Smuggler.SmugglerProgressBase.Counts;
import CountsWithLastEtagAndAttachments = Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithLastEtagAndAttachments;
import CountsWithLastEtag = Raven.Client.Documents.Smuggler.SmugglerProgressBase.CountsWithLastEtag;
import DatabaseRecordProgress = Raven.Client.Documents.Smuggler.SmugglerProgressBase.DatabaseRecordProgress;
import SmugglerProgressBase = Raven.Client.Documents.Smuggler.SmugglerProgressBase;
import RestoreProgress = Raven.Client.ServerWide.Operations.RestoreProgress;
import SmugglerResult = Raven.Client.Documents.Smuggler.SmugglerResult;
import OperationChanged = Raven.Server.NotificationCenter.Notifications.OperationChanged;
import BackupProgress = Raven.Client.Documents.Operations.Backups.BackupProgress;
import CloudUploadStatus = Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
import ShardedSmugglerProgress = Raven.Client.Documents.Smuggler.ShardedSmugglerProgress;
import ShardedBackupResult = Raven.Client.Documents.Operations.Backups.ShardedBackupResult;
import ShardNodeSmugglerResult = Raven.Client.Documents.Smuggler.ShardNodeSmugglerResult;
import ShardNodeBackupResult = Raven.Client.Documents.Operations.Backups.ShardNodeBackupResult;
import BackupResult = Raven.Client.Documents.Operations.Backups.BackupResult;
import { timeAwareEWMA } from "viewmodels/common/timeAwareEWMA";

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
    isNested: boolean;
}

type uploadListItem = {
    name: string;
    uploadProgress: genericProgress;
}

interface ProcessingItem {
    ewma: timeAwareEWMA;
    lastItemsCount: number;
}

class smugglerDatabaseDetails extends abstractOperationDetails {

    view = require("views/common/notificationCenter/detailViewer/operations/smugglerDatabaseDetails.html");

    private static sizeFormatter = generalUtils.formatBytesToSize;

    static extractingDataStageName = "Extracting data";

    detailsVisible = ko.observable<boolean>(false);
    tail = ko.observable<boolean>(true);

    processingItems: dictionary<ProcessingItem> = {};

    canDelay: KnockoutComputed<boolean>;

    exportItems: KnockoutComputed<Array<smugglerListItem>>;
    uploadItems: KnockoutComputed<Array<uploadListItem>>;
    messages: KnockoutComputed<Array<string>>;
    messagesJoined: KnockoutComputed<string>;
    previousProgressMessages: string[];
    processingSpeed: KnockoutComputed<string>;

    filesProcessed: KnockoutComputed<File>;
    
    filesCount: KnockoutComputed<Raven.Client.Documents.Smuggler.SmugglerProgressBase.FileCounts>;

    shardedSmugglerInProgress: KnockoutComputed<boolean>;
    currentShard: KnockoutComputed<number>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.bindToCurrentInstance("toggleDetails");

        this.initObservables();
    }

    private static extractType(value: object): string {
        return value && "$type" in value ? (value as any)["$type"] : null;
    }

    private static isShardedResult(value: object): value is ShardedSmugglerResult {
        const type = smugglerDatabaseDetails.extractType(value);

        if (!type) {
            return false;
        }

        return type.includes("ShardedSmugglerResult") || type.includes("ShardedBackupResult") || type.includes("ShardedRestoreResult");
    }

    private static isShardedProgress(value: object): value is ShardedSmugglerProgress {
        const type = smugglerDatabaseDetails.extractType(value);

        if (!type) {
            return false;
        }

        return type.includes("ShardedSmugglerProgress") || type.includes("ShardedBackupProgress") || type.includes("ShardedRestoreResult");
    }

    private static isShardedBackupResult(value: object): value is ShardedBackupResult {
        const type = smugglerDatabaseDetails.extractType(value);
        if (!type) {
            return false;
        }

        return type.includes("ShardedBackupResult");
    }

    private mergeShardedResult(result: ShardedSmugglerResult | ShardedBackupResult): SmugglerProgressBase {
        const mergeCounts = (items: Counts[]): Counts => {
            return {
                ReadCount: items.reduce((p, c) => p + c.ReadCount, 0),
                ErroredCount: items.reduce((p, c) => p + c.ErroredCount, 0),
                Skipped: items.every(x => x.Skipped),
                Processed: items.some(x => x.Processed),
                SizeInBytes: items.reduce((p, c) => p + c.SizeInBytes, 0),
                StartTime: null,
            }
        }

        const mergeCountsWithLastEtag = (items: CountsWithLastEtag[]): CountsWithLastEtag => {
            return {
                ...mergeCounts(items),
                LastEtag: -1,
            }
        }

        const mergeCountsWithLastEtagAndAttachments = (items: CountsWithLastEtagAndAttachments[]): CountsWithLastEtagAndAttachments => {
            return {
                ...mergeCountsWithLastEtag(items),
                Attachments: mergeCounts(items.map(x => x.Attachments))
            }
        }

        const mergeCountsWithSkippedCountAndLastEtag = (items: CountsWithSkippedCountAndLastEtag[]): CountsWithSkippedCountAndLastEtag => {
            return {
                ...mergeCountsWithLastEtag(items),
                SkippedCount: items.reduce((p, c) => p + c.SkippedCount, 0),
            }
        }

        const mergeCountsWithSkippedCountAndLastEtagAndAttachments = (items: CountsWithSkippedCountAndLastEtagAndAttachments[]): CountsWithSkippedCountAndLastEtagAndAttachments => {
            return {
                ...mergeCountsWithLastEtagAndAttachments(items),
                SkippedCount: items.reduce((p, c) => p + c.SkippedCount, 0),
            }
        }

        const results: Array<ShardNodeSmugglerResult | ShardNodeBackupResult> = result.Results;

        let extraProps: object = {};

        if (smugglerDatabaseDetails.isShardedBackupResult(result)) {
            // noinspection UnnecessaryLocalVariableJS
            const backup: Partial<BackupResult> = {
                SnapshotBackup: mergeCounts(result.Results.map(x => x.Result.SnapshotBackup))
            }

            extraProps = backup;
        }

        return {
            CanMerge: false,
            CompareExchange: mergeCountsWithLastEtag(results.map(x => x.Result.CompareExchange)),
            CompareExchangeTombstones: mergeCounts(results.map(x => x.Result.CompareExchangeTombstones)),
            Conflicts: mergeCountsWithLastEtag(results.map(x => x.Result.Conflicts)),
            Counters: mergeCountsWithSkippedCountAndLastEtag(results.map(x => x.Result.Counters)),
            DatabaseRecord: mergeCounts(results.map(x => x.Result.DatabaseRecord)) as DatabaseRecordProgress,
            Documents: mergeCountsWithSkippedCountAndLastEtagAndAttachments(results.map(x => x.Result.Documents)),
            Identities: mergeCountsWithLastEtag(results.map(x => x.Result.Identities)),
            Indexes: mergeCounts(results.map(x => x.Result.Indexes)),
            ReplicationHubCertificates: mergeCounts(results.map(x => x.Result.ReplicationHubCertificates)),
            RevisionDocuments: mergeCountsWithSkippedCountAndLastEtagAndAttachments(results.map(x => x.Result.RevisionDocuments)),
            Subscriptions: mergeCounts(results.map(x => x.Result.Subscriptions)),
            TimeSeries: mergeCountsWithSkippedCountAndLastEtag(results.map(x => x.Result.TimeSeries)),
            TimeSeriesDeletedRanges: mergeCountsWithSkippedCountAndLastEtag(results.map(x => x.Result.TimeSeriesDeletedRanges)),
            Tombstones: mergeCountsWithLastEtag(results.map(x => x.Result.Tombstones)),
            ...extraProps
        };
    }

    protected initObservables() {
        super.initObservables();

        this.filesCount = ko.pureComputed(() => {
            if (this.op.status() !== "InProgress") {
                return null;
            }

            const status: any = this.op.progress();
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

            let status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as SmugglerProgressBase;
            if (!status) {
                return [];
            }

            if (smugglerDatabaseDetails.isShardedResult(status)) {
                status = this.mergeShardedResult(status);
            }

            const result: smugglerListItem[] = [];
            if ("SnapshotBackup" in status) {
                const backupCount = (status as BackupProgress).SnapshotBackup;

                // skip it this case means it is not backup progress object or it is backup of non-binary data
                if (backupCount && backupCount.Skipped) {
                    result.push(this.mapToExportListItem("Backed up files", backupCount));
                }
            }

            if ("SnapshotRestore" in status) {
                const restoreCounts = (status as RestoreProgress).SnapshotRestore;

                // skip it this case means it is not restore progress object or it is restore of non-binary data 
                if (restoreCounts && restoreCounts.Skipped) {
                    result.push(this.mapToExportListItem("Preparing restore", restoreCounts));
                }
            }

            const isDatabaseMigration = this.op.taskType() === "DatabaseMigration";

            if (this.op.taskType() === "CollectionImportFromCsv" || isDatabaseMigration) {
                result.push(this.mapToExportListItem("Documents", status.Documents));
                if (isDatabaseMigration) {
                    const attachments = (status.Documents as CountsWithLastEtagAndAttachments).Attachments;
                    result.push(this.mapToExportListItem("Attachments", attachments, true));
                }
            } else {
                result.push(this.mapToExportListItem("Database Record", status.DatabaseRecord));
                result.push(this.mapToExportListItem("Documents", status.Documents));

                const attachments = (status.Documents as CountsWithLastEtagAndAttachments).Attachments;
                result.push(this.mapToExportListItem("Attachments", attachments, true));

                result.push(this.mapToExportListItem("Counters", status.Counters, true));
                result.push(this.mapToExportListItem("TimeSeries", status.TimeSeries, true));

                if (this.op.taskType() === "DatabaseImport") {
                    result.push(this.mapToExportListItem("Tombstones", status.Tombstones, true));
                }

                result.push(this.mapToExportListItem("Revisions", status.RevisionDocuments));
                const revisionsAttachments = (status.RevisionDocuments as CountsWithLastEtagAndAttachments).Attachments;
                result.push(this.mapToExportListItem("Attachments", revisionsAttachments, true));

                result.push(this.mapToExportListItem("Conflicts", status.Conflicts));
                result.push(this.mapToExportListItem("Indexes", status.Indexes));
                result.push(this.mapToExportListItem("Identities", status.Identities));
                result.push(this.mapToExportListItem("Compare Exchange", status.CompareExchange));

                if (this.op.taskType() === "DatabaseImport") {
                    result.push(this.mapToExportListItem("Compare Exchange Tombstones", status.CompareExchangeTombstones, true));
                }

                result.push(this.mapToExportListItem("Subscriptions", status.Subscriptions));
                result.push(this.mapToExportListItem("Time Series Deleted Ranges", status.TimeSeriesDeletedRanges));
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

            const status = (this.op.isCompleted() ? this.op.result() : this.op.progress()) as SmugglerProgressBase;
            if (!status) {
                return [];
            }

            if (smugglerDatabaseDetails.isShardedResult(status)) {
                return status.Results.flatMap(shardResult => smugglerDatabaseDetails.mapUploadItems(shardResult.Result));
            } else {
                return smugglerDatabaseDetails.mapUploadItems(status);
            }
        });

        this.messages = ko.pureComputed(() => {
            if (this.operationFailed()) {
                const errors = this.errorMessages();
                const previousMessages = this.previousProgressMessages || [];
                return previousMessages.concat(...errors);
            } else if (this.op.isCompleted()) {
                const result = this.op.result() as SmugglerResult;

                if (smugglerDatabaseDetails.isShardedResult(result)) {
                    return result.Results.flatMap(x => x.Result.Messages);
                } else {
                    return result ? result.Messages : [];
                }
            } else {
                const progress = this.op.progress() as SmugglerResult;
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

        this.shardedSmugglerInProgress = ko.pureComputed(() => smugglerDatabaseDetails.isShardedProgress(this.op.progress() as object));

        this.currentShard = ko.pureComputed(() => {
            const progress = this.op.progress() as object;
            if (!smugglerDatabaseDetails.isShardedProgress(progress)) {
                return null;
            }

            return progress.ShardNumber;
        });
    }

    private static mapUploadItems(status: SmugglerProgressBase): uploadListItem[] {
        const result: uploadListItem[] = [];
        if ("S3Backup" in status) {
            const s3BackupStatus = (status as BackupProgress).S3Backup;
            const backupStatus = s3BackupStatus as CloudUploadStatus;
            result.push(smugglerDatabaseDetails.mapUploadItem("S3", backupStatus));
        }

        if ("AzureBackup" in status) {
            const azureBackupStatus = (status as BackupProgress).AzureBackup;
            const backupStatus = azureBackupStatus as CloudUploadStatus;
            result.push(smugglerDatabaseDetails.mapUploadItem("Azure", backupStatus));
        }

        if ("GoogleCloudBackup" in status) {
            const googleCloudBackupStatus = (status as BackupProgress).GoogleCloudBackup;
            const backupStatus = googleCloudBackupStatus as CloudUploadStatus;
            result.push(smugglerDatabaseDetails.mapUploadItem("Google Cloud", backupStatus));
        }

        if ("GlacierBackup" in status) {
            const glacierBackupStatus = (status as BackupProgress).GlacierBackup;
            const backupStatus = glacierBackupStatus as CloudUploadStatus;
            result.push(smugglerDatabaseDetails.mapUploadItem("Glacier", backupStatus));
        }

        if ("FtpBackup" in status) {
            const ftpBackupStatus = (status as BackupProgress).FtpBackup;
            const backupStatus = ftpBackupStatus as CloudUploadStatus;
            result.push(smugglerDatabaseDetails.mapUploadItem("FTP", backupStatus));
        }

        return result.filter(x => x);
    }

    private static mapUploadItem(backupType: string, backupStatus: CloudUploadStatus): uploadListItem | null {
        if (!backupStatus || backupStatus.Skipped) {
            return null;
        }

        const uploadProgress = new genericProgress(
            backupStatus.UploadProgress.UploadedInBytes,
            backupStatus.UploadProgress.TotalInBytes,
            (number: number) => this.sizeFormatter(number),
            backupStatus.UploadProgress.BytesPutsPerSec);

        return {
            name: `Upload to ${backupType}`,
            uploadProgress: uploadProgress
        };
    }

    delayBackup(duration: string) {
        if (!this.canDelay()) {
            return;
        }

        this.close();

        const durationFormatted = generalUtils.formatTimeSpan(duration, true);

        viewHelpers.confirmationMessage("Delay backup", "Do you want to delay backup by " + durationFormatted + "?", {
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

    private static scrollDown() {
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
                smugglerDatabaseDetails.scrollDown();
            }
        }));

        this.registerDisposable(this.tail.subscribe(enabled => {
            if (enabled) {
                smugglerDatabaseDetails.scrollDown();
            }
        }));
    }

    private mapToExportListItem(name: string, item: Counts, isNested = false): smugglerListItem {
        if (!item) {
            return null;
        }

        let stage: smugglerListItemStatus = "processing";

        if (item.Skipped) {
            stage = "skipped";
        } else if (item.Processed) {
            stage = "processed";
            if (item.ErroredCount) {
                stage = "processedWithErrors";
            }
        }

        const hasSkippedCount = "SkippedCount" in item;
        const skippedCount = hasSkippedCount ? (item as CountsWithSkippedCountAndLastEtag).SkippedCount : 0;

        if (smugglerDatabaseDetails.showSpeed(name) && item.StartTime) {
            
            if (stage === "processed" && this.processingItems[name]) {
                this.processingItems[name].ewma.reset();
                this.processingItems[name] = null;
            }

            if (stage === "processing") {
                const itemsCount = item.ReadCount + skippedCount + item.ErroredCount;

                if (!this.processingItems[name]) {
                    this.processingItems[name] = {
                        // 2 seconds halfLife with average incoming data 1-3 seconds seems reasonable
                        ewma: new timeAwareEWMA(2_000),
                        lastItemsCount: 0
                    }
                }

                const itemsDifference = itemsCount - this.processingItems[name].lastItemsCount;

                this.processingItems[name].ewma.handleServerTick(itemsDifference);
                this.processingItems[name].lastItemsCount = itemsCount;
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
            isNested: isNested
        };
    }

    getProcessingSpeed(name: string) {
        if (!this.processingItems[name]) {
            return null;
        }

        return this.processingItems[name].ewma.value();
    }

    private static showSpeed(name: string) {
        return name === "Documents" || name === "Revisions" || name === "Counters" || name === "TimeSeries";
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

    static merge(existing: operation, incoming: OperationChanged): boolean {
        if (!smugglerDatabaseDetails.supportsDetailsFor(existing)) {
            return false;
        }

        const isUpdate = !_.isUndefined(incoming);

        if (!isUpdate) {
            // object was just created  - only copy message -> message field

            if (!existing.isCompleted()) {
                const result = existing.progress() as SmugglerResult;
                result.Messages = result.Message ? [result.Message] : [];
            }

        } else if (incoming.State.Status === "InProgress") { // if incoming operation is in progress, then merge messages into existing item
            const incomingResult = incoming.State.Progress as SmugglerResult;
            const existingResult = existing.progress() as SmugglerResult;

            if (incomingResult.Message) {
                incomingResult.Messages = existingResult.Messages.concat(incomingResult.Message);
            }
        }

        if (isUpdate) {
            existing.updateWith(incoming);
        }

        return true;
    }

    copyLogs() {
        const dialogContainer = document.getElementById("exportModal");
        copyToClipboard.copy(this.messagesJoined(), "Copied details to clipboard.", dialogContainer);
    }
}

export = smugglerDatabaseDetails;
