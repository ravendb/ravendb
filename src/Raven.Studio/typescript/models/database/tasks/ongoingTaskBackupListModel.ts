/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

class ongoingTaskBackupListModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;
    activeDatabase = activeDatabaseTracker.default.database;

    backupType = ko.observable<Raven.Client.ServerWide.PeriodicBackup.BackupType>();
    nextBackup = ko.observable<string>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();

    showBackupDetails = ko.observable(false);
    textClass = ko.observable<string>();

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskBackup) {
        super();

        this.isInTasksListView = true;
        this.update(dto);
        this.initializeObservables();
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
        }

        if (dto.LastIncrementalBackup) {
            const lastIncrementalBackup = moment.utc(dto.LastIncrementalBackup).local().format(dateFormat);
            this.lastIncrementalBackup(lastIncrementalBackup);
        }

        if (dto.NextBackup) {
            const now = moment();
            const timeSpan = moment.duration(dto.NextBackup.TimeSpan);
            const nextBackupDateTime = now.add(timeSpan).format(dateFormat);
            this.nextBackup(`${nextBackupDateTime} (${dto.NextBackup.IsFull ? "Full" : "Incremental"})`);
            this.textClass("text-details");
        } else {
            this.nextBackup("N/A");
            this.textClass("text-warning");
        }
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    toggleDetails() {
        this.showBackupDetails(!this.showBackupDetails());

        if (this.showBackupDetails()) {
            this.refreshBackupInfo();
        } 
    }

    refreshBackupInfo() {
        ongoingTaskInfoCommand.forBackup(this.activeDatabase(), this.taskId)
            .execute()
            .done((result: Raven.Client.ServerWide.Operations.OngoingTaskBackup) => {
                this.update(result);
            });
    }
}

export = ongoingTaskBackupListModel;
