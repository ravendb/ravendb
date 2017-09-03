/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 

class ongoingTaskBackupModel extends ongoingTask {
    editUrl: KnockoutComputed<string>;

    backupType = ko.observable<Raven.Client.ServerWide.PeriodicBackup.BackupType>();
    nextBackup = ko.observable<string>();
    lastFullBackup = ko.observable<string>();
    lastIncrementalBackup = ko.observable<string>();

    constructor(dto: Raven.Client.ServerWide.Operations.OngoingTaskBackup, isInListView: boolean) {
        super();

        this.isInTasksListView = isInListView;
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
            const lastIncrementalBackup = moment
                .utc(dto.LastIncrementalBackup)
                .local()
                .format(dateFormat);
            this.lastIncrementalBackup(lastIncrementalBackup);
        }

        if (dto.NextBackup) {
            const now = moment();
            const timeSpan = moment.duration(dto.NextBackup.TimeSpan);
            const nextBackupDateTime = now.add(timeSpan).format(dateFormat);
            this.nextBackup(`${nextBackupDateTime} (${dto.NextBackup.IsFull ? "Full" : "Incremental"})`);
        } else {
            this.nextBackup("N/A");
        }
    }

    editTask() {
        router.navigate(this.editUrl());
    }

    generateTaskName(dto: Raven.Client.ServerWide.Operations.OngoingTaskBackup): string {
        return dto.BackupDestinations.length === 0 ? "No destinations" : `${dto.BackupType} to ${dto.BackupDestinations.join(", ")}`;
    }
}

export = ongoingTaskBackupModel;
