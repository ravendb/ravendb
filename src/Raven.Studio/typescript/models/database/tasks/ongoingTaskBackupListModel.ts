/// <reference path="../../../../typings/tsd.d.ts"/>
import app = require("durandal/app");
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import backupNowCommand = require("commands/database/tasks/backupNowCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");
import backupNow = require("viewmodels/database/tasks/backupNow");

class ongoingTaskBackupListModel extends ongoingTask {
    private static neverBackedUpText = "Never backed up";

    editUrl: KnockoutComputed<string>;
    activeDatabase = activeDatabaseTracker.default.database;

    backupType = ko.observable<Raven.Client.ServerWide.PeriodicBackup.BackupType>();
    nextBackup = ko.observable<string>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();
    ongoingBackup = ko.observable<string>();

    showBackupDetails = ko.observable(false);
    textClass = ko.observable<string>();

    noBackupDestinations = ko.observable<boolean>(false);
    backupNowInProgress = ko.observable<boolean>(false);
    disabledBackupNowReason = ko.observable<string>();
    isBackupNowEnabled: KnockoutComputed<boolean>;
    neverBackedUp = ko.observable<boolean>(false);
    fullBackupTypeName: KnockoutComputed<string>;

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskBackup) {
        super();

        this.update(dto);
        this.initializeObservables();

        this.isBackupNowEnabled = ko.pureComputed(() => {
            if (this.nextBackup() === "N/A") {
                this.disabledBackupNowReason("No backup destinations");
                return false;
            }

            if (this.ongoingBackup()) {
                this.disabledBackupNowReason("Backup is already in progress");
                return false;
            }

            this.disabledBackupNowReason(null);
            return true;
        });

        this.fullBackupTypeName = ko.pureComputed(() => this.getBackupType(this.backupType(), true));
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editPeriodicBackupTask(this.taskId); 
    }

    update(dto: Raven.Client.ServerWide.Operations.OngoingTaskBackup) {
        super.update(dto);

        this.backupType(dto.BackupType);
        const dateFormat = "YYYY MMMM Do, h:mm A";

        if (dto.LastFullBackup) {
            const lastFullBackup = moment.utc(dto.LastFullBackup).local().format(dateFormat);
            this.lastFullBackup(lastFullBackup);
            this.neverBackedUp(false);
        } else {
            this.lastFullBackup(ongoingTaskBackupListModel.neverBackedUpText);
            this.neverBackedUp(true);
        }

        if (dto.LastIncrementalBackup) {
            const lastIncrementalBackup = moment.utc(dto.LastIncrementalBackup).local().format(dateFormat);
            this.lastIncrementalBackup(lastIncrementalBackup);
        } else {
            this.lastIncrementalBackup(ongoingTaskBackupListModel.neverBackedUpText);
        }

        if (dto.OnGoingBackup) {
            const ongoingBackup = moment.utc(dto.OnGoingBackup.StartTime).local().format(dateFormat);
            this.ongoingBackup(`${ongoingBackup} (${this.getBackupType(dto.BackupType, dto.OnGoingBackup.IsFull)})`);
        } else {
            this.ongoingBackup(null);
        }

        if (dto.NextBackup) {
            const now = moment();
            const timeSpan = moment.duration(dto.NextBackup.TimeSpan);
            const nextBackupDateTime = now.add(timeSpan).format(dateFormat);
            this.nextBackup(`${nextBackupDateTime} (${this.getBackupType(dto.BackupType, dto.NextBackup.IsFull)})`);
            this.textClass("text-details");
        } else {
            this.nextBackup("N/A");
            this.textClass("text-warning");
        }
    }

    private getBackupType(backupType: Raven.Client.ServerWide.PeriodicBackup.BackupType, isFull: boolean): string {
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
        this.showBackupDetails.toggle();

        if (this.showBackupDetails()) {
            this.refreshBackupInfo();
        } 
    }

    refreshBackupInfo() {
        return ongoingTaskInfoCommand.forBackup(this.activeDatabase(), this.taskId)
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.OngoingTaskBackup) => this.update(result));
    }

    backupNow() {
        const confirmDeleteViewModel = new backupNow(this.getBackupType(this.backupType(), true));

        confirmDeleteViewModel
            .result
            .done((confirmResult: backupNowConfirmResult) => {
                if (confirmResult.can) {
                    this.backupNowInProgress(true);

                    const task = new backupNowCommand(this.activeDatabase(), this.taskId, confirmResult.isFullBackup);
                    task.execute().always(() => this.refreshBackupInfo().always(() => this.backupNowInProgress(false)));
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }
}

export = ongoingTaskBackupListModel;
