/// <reference path="../../../../../typings/tsd.d.ts"/>
import appUrl = require("common/appUrl");
import serverWideTaskListModel = require("models/database/tasks/serverWide/serverWideTaskListModel");
import shell = require("viewmodels/shell");
import getAllServerWideTasksCommand = require("commands/serverWide/tasks/getAllServerWideTasksCommand");
import generalUtils = require("common/generalUtils");

class serverWideExternalReplicationListModel extends serverWideTaskListModel {
    
    delayTimeText = ko.observable<string>();

    constructor(dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask) {
        super();
        
        this.update(dto);
        this.initializeObservables();
    }

    initializeObservables() {
        super.initializeObservables();

        this.editUrl = ko.pureComputed(() => appUrl.forEditServerWideExternalReplication(this.taskName()));
    }
    
    // dto param is union-type only so that it compiles - due to class inheritance....
    update(dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask |
                Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask) {
        super.update(dto as Raven.Client.Documents.Operations.OngoingTasks.OngoingTask);

        const serverWideExternalReplicationTask = dto as Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask;
        
        const delayTime = generalUtils.timeSpanToSeconds(serverWideExternalReplicationTask.DelayReplicationFor);
        this.delayTimeText(delayTime ? generalUtils.formatTimeSpan(delayTime * 1000, true) : "No delay");
        this.excludedDatabases(serverWideExternalReplicationTask.ExcludedDatabases);
        this.isServerWide(true);
    }

    toggleDetails() {
        this.showDetails.toggle();

        if (this.showDetails()) {
            this.refreshExternalReplicationInfo();
        } 
    }

    refreshExternalReplicationInfo() {
        if (shell.showConnectionLost()) {
            // looks like we don't have connection to server, skip index progress update 
            return $.Deferred<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>().fail();
        }

        return new getAllServerWideTasksCommand(this.taskName(), "Replication")
            .execute()
            .done((result: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult) =>
                this.update(result.Tasks[0]));
    }
}

export = serverWideExternalReplicationListModel;
