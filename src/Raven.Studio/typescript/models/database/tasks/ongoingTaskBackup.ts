/// <reference path="../../../../typings/tsd.d.ts"/>

import ongoingTask = require("models/database/tasks/ongoingTask"); 

class ongoingTaskBackup extends ongoingTask {

    backupType = ko.observable<Raven.Server.Web.System.BackupType>();
    backupDestinations = ko.observableArray<string>();

    constructor(dto: Raven.Server.Web.System.OngoingTaskBackup) {
        super(dto);
        this.initializeObservables();
        this.update(dto);
    }

    initializeObservables() {
        super.initializeObservables();
        // ...
    }

    update(dto: Raven.Server.Web.System.OngoingTaskBackup) {
        super.update(dto);
        this.backupType(dto.BackupType);
        this.backupDestinations(dto.BackupDestinations);
    }

    enableTask() {
        alert("enabling task backup");
        // ...
    }

    disableTask() {
        alert("disabling task backup");
        // ...
    }

    editTask() {
        alert("edit task backup");
        // ...
    }

    removeTask() {
        alert("remove task backup");
        // ...
    }
}

export = ongoingTaskBackup;
