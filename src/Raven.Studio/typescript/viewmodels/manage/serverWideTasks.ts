import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getAllServerWideTasksCommand = require("commands/serverWide/tasks/getAllServerWideTasksCommand");
import serverWideBackupListModel = require("models/database/tasks/serverWide/serverWideBackupListModel");
import serverWideExternalReplicationListModel = require("models/database/tasks/serverWide/serverWideExternalReplicationListModel");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import deleteServerWideTaskCommand = require("commands/serverWide/tasks/deleteServerWideTaskCommand");
import toggleServerWideTaskCommand = require("commands/serverWide/tasks/toggleServerWideTaskCommand");
import createServerWideTask = require("viewmodels/manage/createServerWideTask");

class serverWideTasks extends viewModelBase {
    serverWideBackupTasks = ko.observableArray<serverWideBackupListModel>();
    serverWideExternalReplicationTasks = ko.observableArray<serverWideExternalReplicationListModel>();
   
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveServerWideTask", "confirmEnableServerWideTask", "confirmDisableServerWideTask", "toggleDetails");
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        
        return this.fetchServerWideTasks();
    }

    attached() {
        super.attached();
        
        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchReconnect(() => this.refresh()));

        this.updateUrl(appUrl.forServerWideTasks());
    }
    
    private refresh() {
        return this.fetchServerWideTasks();
    }

    private fetchServerWideTasks(): JQueryPromise<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult> { 
        return new getAllServerWideTasksCommand() 
            .execute()
            .done((info) => {
                this.processTasksResult(info);
            });
    }

    toggleDetails(item: ongoingTaskListModel) {
        item.toggleDetails();
    }
    
    private processTasksResult(result: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult) {
        const oldTasks = [
            ...this.serverWideExternalReplicationTasks(),
            ...this.serverWideBackupTasks()
           ] as Array<{ taskId: number }>;
        
        const oldTaskIds = oldTasks.map(x => x.taskId);
        const newTaskIds = result.Tasks.map(x => x.TaskId);
        const toDeleteIds = _.without(oldTaskIds, ...newTaskIds);

        const groupedTasks = _.groupBy(result.Tasks, x => x.TaskType);
        
        this.mergeTasks(this.serverWideExternalReplicationTasks,
            groupedTasks["Replication" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType],
            toDeleteIds,
            (dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideExternalReplicationTask) => new serverWideExternalReplicationListModel(dto));
        
        this.mergeTasks(this.serverWideBackupTasks,
            groupedTasks["Backup" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType],
            toDeleteIds,
            (dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask) => new serverWideBackupListModel(dto));
    }

    private mergeTasks<T extends ongoingTaskListModel>(container: KnockoutObservableArray<T>,
                                                       incomingData: Array<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask>,
                                                       toDelete: Array<number>,
                                                       ctr: (dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask) => T) {
        // remove old tasks
        container()
            .filter(x => _.includes(toDelete, x.taskId))
            .forEach(task => container.remove(task));

        // update existing or add new tasks
        (incomingData || []).forEach(item => {
            const existingItem = container().find(x => x.taskId === item.TaskId);
            if (existingItem) {
                existingItem.update(item);
            } else {
                const newItem = ctr(item);
                const insertIdx = _.sortedIndexBy(container(), newItem, x => x.taskName().toLocaleLowerCase());
                container.splice(insertIdx, 0, newItem);
            }
        })
    }

    confirmEnableServerWideTask(model: ongoingTaskModel) {
        this.confirmationMessage("Enable Server-Wide Task", 
            `You're enabling Server-Wide ${model.taskType()} task:<br><strong>${model.taskName()}</strong>`, {
            buttons: ["Cancel", "Enable"], 
            html: true
        })
            .done(result => {
                if (result.can) {
                    new toggleServerWideTaskCommand(model.taskType(), model.taskName(), false)
                        .execute()
                        .done(() => model.taskState("Enabled"))
                        .always(() => this.fetchServerWideTasks());
                }
            });
    }

    confirmDisableServerWideTask(model: ongoingTaskModel) {
        this.confirmationMessage("Disable Server-Wide Task",
            `You're disabling Server-Wide ${model.taskType()} task:<br><strong>${model.taskName()}</strong>`, {
            buttons: ["Cancel", "Disable"],
            html: true
        })
            .done(result => {
                if (result.can) {
                    new toggleServerWideTaskCommand(model.taskType(), model.taskName(), true)
                        .execute()
                        .done(() => model.taskState("Disabled"))
                        .always(() => this.fetchServerWideTasks()); 
                }
            });
    }

    confirmRemoveServerWideTask(model: ongoingTaskModel) {
        const taskType = ongoingTaskModel.mapTaskType(model.taskType());

        this.confirmationMessage("Delete Task", "You're deleting " + taskType + ": " + model.taskName(), {
            buttons: ["Cancel", "Delete"]
        })
            .done(result => {
                if (result.can) {
                    this.deleteServerWideTask(model);
                }
            });
    }

    private deleteServerWideTask(model: ongoingTaskModel) {
        new deleteServerWideTaskCommand(model.taskType(), model.taskName())
            .execute()
            .done(() => this.fetchServerWideTasks());
    }

    addNewServerWideTask() {
        const addOngoingTaskView = new createServerWideTask();
        app.showBootstrapDialog(addOngoingTaskView);
    }
}

export = serverWideTasks;
