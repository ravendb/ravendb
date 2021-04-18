/// <reference path="../../../../typings/tsd.d.ts"/>
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel"); 
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import backupNowPeriodicCommand = require("commands/database/tasks/backupNowPeriodicCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import backupNow = require("viewmodels/database/tasks/backupNow");
import generalUtils = require("common/generalUtils");
import timeHelpers = require("common/timeHelpers");
import notificationCenter = require("common/notifications/notificationCenter");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import shell = require("viewmodels/shell");
import accessManager = require("common/shell/accessManager");

class ongoingTaskBackupListModel extends ongoingTaskListModel {
    private static neverBackedUpText = "Never backed up";
    
    static serverWideNamePrefixFromServer = "Server Wide Backup";
    
    activeDatabase = activeDatabaseTracker.default.database;
    
    private watchProvider: (task: ongoingTaskBackupListModel) => void;

    backupType = ko.observable<Raven.Client.Documents.Operations.Backups.BackupType>();
    nextBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.NextBackup>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();
    onGoingBackup = ko.observable<Raven.Client.Documents.Operations.OngoingTasks.RunningBackup>();
    retentionPolicyPeriod = ko.observable<string>();
    retentionPolicyDisabled = ko.observable<boolean>();

    textClass = ko.observable<string>();

    backupDestinations = ko.observableArray<string>([]);
    backupNowInProgress = ko.observable<boolean>(false);
    isRunningOnAnotherNode: KnockoutComputed<boolean>;
    disabledBackupNowReason = ko.observable<string>();
    isBackupNowEnabled: KnockoutComputed<boolean>;
    isBackupNowVisible: KnockoutComputed<boolean>;
    neverBackedUp = ko.observable<boolean>(false);
    fullBackupTypeName: KnockoutComputed<string>;
    isBackupEncrypted = ko.observable<boolean>();
    lastExecutingNode = ko.observable<string>();

    backupDestinationsHumanized: KnockoutComputed<string>;
    lastFullBackupHumanized: KnockoutComputed<string>;
    lastIncrementalBackupHumanized: KnockoutComputed<string>;
    nextBackupHumanized: KnockoutComputed<string>;
    onGoingBackupHumanized: KnockoutComputed<string>;
    retentionPolicyHumanized: KnockoutComputed<string>;
    throttledRefreshBackupInfo: () => void;
    
    constructor(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup, watchProvider: (task: ongoingTaskBackupListModel) => void) {
        super();
        
        this.throttledRefreshBackupInfo = _.throttle(() => this.refreshBackupInfo(false), 60 * 1000);
        
        this.watchProvider = watchProvider;
        this.update(dto);
        
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();
        
        this.editUrl = ko.pureComputed(()=> {
             const urls = appUrl.forCurrentDatabase();

             return urls.editPeriodicBackupTask(this.taskId)();
        });

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

        this.isBackupNowVisible = ko.pureComputed(() => {
            return  !this.isServerWide() || accessManager.default.isClusterAdminOrClusterNodeClearance();
        });
        
        this.lastFullBackupHumanized = ko.pureComputed(() => {
            const lastFullBackup = this.lastFullBackup();
            if (!lastFullBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            return generalUtils.formatDurationByDate(moment.utc(lastFullBackup), true);
        });

        this.lastIncrementalBackupHumanized = ko.pureComputed(() => {
            const lastIncrementalBackup = this.lastIncrementalBackup();
            if (!lastIncrementalBackup) {
                return ongoingTaskBackupListModel.neverBackedUpText;
            }

            return generalUtils.formatDurationByDate(moment.utc(lastIncrementalBackup), true);
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
            
            if (diff <= 0 && this.showDetails()) {
                this.throttledRefreshBackupInfo();
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
            return `${fromDuration} (${this.getBackupType(this.backupType(), onGoingBackup.IsFull)})`;
        });

        this.retentionPolicyHumanized = ko.pureComputed(() => {
            return this.retentionPolicyDisabled() ? "No backups will be removed" : generalUtils.formatTimeSpan(this.retentionPolicyPeriod(), true);
        });
        
        this.backupDestinationsHumanized =  ko.pureComputed(() => {
            return this.backupDestinations().length ? this.backupDestinations().join(', ') : "No destinations defined";
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

    update(dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) {
        super.update(dto);

        this.backupType(dto.BackupType);
        this.backupDestinations(dto.BackupDestinations);
        
        this.neverBackedUp(!dto.LastFullBackup);
        this.lastFullBackup(dto.LastFullBackup);
        this.lastIncrementalBackup(dto.LastIncrementalBackup);
        this.nextBackup(dto.NextBackup);
        this.onGoingBackup(dto.OnGoingBackup);
        this.isBackupEncrypted(dto.IsEncrypted);
        this.lastExecutingNode(dto.LastExecutingNodeTag || "N/A");

        // Check backward compatibility
        this.retentionPolicyDisabled(dto.RetentionPolicy ? dto.RetentionPolicy.Disabled : true);
        this.retentionPolicyPeriod(dto.RetentionPolicy ? dto.RetentionPolicy.MinimumBackupAgeToKeep : "0.0:00:00");
        
        if (this.onGoingBackup()) {
            this.watchProvider(this);
        }

        this.backupNowInProgress(!!this.onGoingBackup());
        
        this.isServerWide(this.taskName().startsWith(ongoingTaskBackupListModel.serverWideNamePrefixFromServer));
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

        const backupNowViewModel = new backupNow(this.getBackupType(this.backupType(), true));
        backupNowViewModel
            .result
            .done((confirmResult: backupNowConfirmResult) => {
                if (confirmResult.can) {
                    this.backupNowInProgress(true);

                    const task = new backupNowPeriodicCommand(this.activeDatabase(), this.taskId, confirmResult.isFullBackup, this.taskName());
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
                        })
                        .fail(() => {
                            // we failed to start the backup task
                            this.backupNowInProgress(false);
                        });
                        // backupNowInProgress is set to false after operation is finished
                }
            });

        app.showBootstrapDialog(backupNowViewModel);
    }
}

export = ongoingTaskBackupListModel;
