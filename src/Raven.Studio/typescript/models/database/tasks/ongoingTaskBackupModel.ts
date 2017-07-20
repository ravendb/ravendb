/// <reference path="../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import router = require("plugins/router");
import ongoingTask = require("models/database/tasks/ongoingTaskModel"); 


class ongoingTaskBackupModel extends ongoingTask {

    editUrl: KnockoutComputed<string>;

    backupType = ko.observable<Raven.Client.Server.PeriodicBackup.BackupType>();
    backupDestinations = ko.observableArray<string>();

    constructor(dto: Raven.Server.Web.System.OngoingTaskBackup) {
        super();
        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        const urls = appUrl.forCurrentDatabase();
        this.editUrl = urls.editPeriodicBackupTask(this.taskId); 
    }

    update(dto: Raven.Server.Web.System.OngoingTaskBackup) {
        super.update(dto);
        this.backupType(dto.BackupType);
        this.backupDestinations(dto.BackupDestinations.length === 0 ? ["No destinations"] : dto.BackupDestinations);
    }

    editTask() {
        router.navigate(this.editUrl());
    }
}

export = ongoingTaskBackupModel;
