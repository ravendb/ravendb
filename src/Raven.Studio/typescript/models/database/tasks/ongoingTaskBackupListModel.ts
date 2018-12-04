/// <reference path="../../../../typings/tsd.d.ts"/>
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import backupNowCommand = require("commands/database/tasks/backupNowCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import backupNow = require("viewmodels/database/tasks/backupNow");
import generalUtils = require("common/generalUtils");
import timeHelpers = require("common/timeHelpers");
import notificationCenter = require("common/notifications/notificationCenter");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import shell = require("viewmodels/shell");

class ongoingTaskBackupListModel extends ongoingTaskListModel {
    private static neverBackedUpText = "Never backed up";

    editUrl: KnockoutComputed<string>;
    activeDatabase = activeDatabaseTracker.default.database;
    
    private watchProvider: (task: ongoingTaskBackupListModel) => void;

    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();
    nextBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.NextBackup>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();
    onGoingBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.RunningBackup>();

    textClass = ko.observable<string>();

    backupDestinations = ko.observableArray<string>([]);
    backupNowInProgress = ko.observable<boolean>(false);
    isRunningOnAnotherNode: KnockoutComputed<boolean>;
    disabledBackupNowReason = ko.observable<string>();
    isBackupNowEnabled: KnockoutComputed<boolean>;
    neverBackedUp = ko.observable<boolean>(false);
    fullBackupTypeName: KnockoutComputed<string>;

    lastFullBackupHumanized: KnockoutComputed<string>;
    lastIncrementalBackupHumanized: KnockoutComputed<string>;
    nextBackupHumanized: KnockoutComputed<string>;
    onGoingBackupHumanized: KnockoutComputed<string>;

    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup, watchProvider: (task: ongoingTaskBackupListModel) => void) {
        super();
        this.watchProvider = watchProvider;

        this.update(dto);
        this.initializeObservables();

        this.isBackupNowEnabled = ko.pureComputed(() => {
            if (this.nextBackupHumanized() === "N/A") {
                this.disabledBackupNowReason("No backup destinations");
                return false;
            }

            if (this.isRunningOnAnotherNode()) {
                // the backup is running on another node
                this.disabledBackupNowReason(`Backup in progress on node ${this.responsibleNode().NodeTag}`);
                return false;
            }

            this.disabledBackupNowReason(null);
            return true;
        });

        this.lastFullBackupHumanized = ko.pureComputed(() => {
            const lastFullBackup = this.lastFullBackup();
            if (!lastFullBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            const fromDuration = generalUtils.formatDurationByDate(moment.utc(lastFullBackup), true);
            return `${fromDuration} ago`;
        });

        this.lastIncrementalBackupHumanized = ko.pureComputed(() => {
            const lastIncrementalBackup = this.lastIncrementalBackup();
            if (!lastIncrementalBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            const fromDuration = generalUtils.formatDurationByDate(moment.utc(lastIncrementalBackup), true);
            return `${fromDuration} ago`;
        });

        this.nextBackupHumanized = ko.pureComputed(() => {
            const nextBackup = this.nextBackup();
            if (!nextBackup) {
                this.textClass("text-warning");
                return "N/A";
            }

            if (this.isRunningOnAnotherNode()) {
                this.textClass("text-info");
                // the backup is running on another node
                return `Backup is already running or should start shortly on node ${this.responsibleNode().NodeTag}`;
            }

            this.textClass("text-details");
            const now = timeHelpers.utcNowWithSecondPrecision();
            const diff = moment.utc(nextBackup.DateTime).diff(now);
            if (diff <= 0) {
                this.refreshBackupInfo(true);
            }

            const backupType = this.getBackupType(this.backupType(), nextBackup.IsFull);
            const backupTypeText = backupType !== "Snapshot" ? `${backupType} Backup` : backupType;
            const formatDuration = generalUtils.formatDuration(moment.duration(diff), true, 2, true);
            return `in ${formatDuration} (${backupTypeText})`;
        });

        this.onGoingBackupHumanized = ko.pureComputed(() => {
            const onGoingBackup = this.onGoingBackup();
            if (!onGoingBackup) {
                return null;
            }

            const fromDuration = generalUtils.formatDurationByDate(moment.utc(onGoingBackup.StartTime), true);
            return `${fromDuration} ago (${this.getBackupType(this.backupType(), onGoingBackup.IsFull)})`;
        });

        this.isRunningOnAnotherNode = ko.pureComputed(() => {
            const responsibleNode = this.responsibleNode();
            if (!responsibleNode || !responsibleNode.NodeTag) {
                return false;
            }

            if (responsibleNode.NodeTag === clusterTopologyManager.default.localNodeTag()) {
                return false;
            }

            const nextBackup = this.nextBackup();
            if (!nextBackup) {
                return false;
            }

            const now = timeHelpers.utcNowWithSecondPrecision();
            const diff = moment.utc(nextBackup.DateTime).diff(now);
            return diff <= 0;
        });

        this.fullBackupTypeName = ko.pureComputed(() => this.getBackupType(this.backupType(), true));
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editPeriodicBackupTask(this.taskId); 
    }

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) {
        super.update(dto);

        this.backupType(dto.BackupType);
        this.backupDestinations(dto.BackupDestinations);
        
        this.neverBackedUp(!dto.LastFullBackup);
        this.lastFullBackup(dto.LastFullBackup);
        this.lastIncrementalBackup(dto.LastIncrementalBackup);
        this.nextBackup(dto.NextBackup);
        this.onGoingBackup(dto.OnGoingBackup);

        if (this.onGoingBackup()) {
            this.watchProvider(this);
        }

        this.backupNowInProgress(!!this.onGoingBackup());
    }

    private getBackupType(backupType: Raven.Client.Documents.Operations.Backups.BackupType, isFull: boolean): string {
        if (!isFull) {
            return "Incremental";
        }

        if (backupType === "Snapshot") {
            return "Snapshot";
        }

        return "Full";
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showDetails.toggle();

        if (this.showDetails()) {
            this.refreshBackupInfo(true);
        } 
    }

    refreshBackupInfo(reportFailure: boolean) {
        if (shell.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>().fail();
        }

        return ongoingTaskInfoCommand.forBackup(this.activeDatabase(), this.taskId, reportFailure)
            .execute()
            .done((result: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => this.update(result));
    }

    backupNow() {
        const db = this.activeDatabase();
        const onGoingBackup = this.onGoingBackup();
        if (onGoingBackup) {
            const runningOperationId = onGoingBackup.RunningBackupTaskId;
            if (runningOperationId) {
                notificationCenter.instance.openDetailsForOperationById(db, runningOperationId);
                return;
            }
        }

        const confirmDeleteViewModel = new backupNow(this.getBackupType(this.backupType(), true));
        confirmDeleteViewModel
            .result
            .done((confirmResult: backupNowConfirmResult) => {
                if (confirmResult.can) {
                    this.backupNowInProgress(true);

                    const task = new backupNowCommand(this.activeDatabase(), this.taskId, confirmResult.isFullBackup, this.taskName());
                    task.execute()
                        .done((backupNowResult: Raven.Client.Documents.Operations.Backups.StartBackupOperationResult) => {
                            this.refreshBackupInfo(true);
                            this.watchProvider(this);

                            if (backupNowResult && clusterTopologyManager.default.localNodeTag() === backupNowResult.ResponsibleNode) {
                                // running on this node
                                const operationId = backupNowResult.OperationId;
                                if (!this.onGoingBackup()) {
                                    this.onGoingBackup({
                                        IsFull: confirmResult.isFullBackup,
                                        RunningBackupTaskId: operationId
                                    });
                                }
                                notificationCenter.instance.openDetailsForOperationById(db, operationId);
                            }
                        });
                        // backupNowInProgress is set to false after operation is finished
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }
}

export = ongoingTaskBackupListModel;
