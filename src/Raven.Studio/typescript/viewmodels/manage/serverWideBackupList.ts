import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import getAllServerWideTasksCommand = require("commands/resources/getAllServerWideTasksCommand");
import ongoingTaskServerWideBackupListModel = require("models/database/tasks/ongoingTaskServerWideBackupListModel");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import deleteServerWideTaskCommand = require("commands/resources/deleteServerWideTaskCommand");
import toggleServerWideTaskCommand = require("commands/resources/toggleServerWideTaskCommand");

class serverWideBackupList extends viewModelBase {

    serverWideBackupTasks = ko.observableArray<ongoingTaskServerWideBackupListModel>();     
   
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveServerWideBackup", "confirmEnableServerWideBackup", "confirmDisableServerWideBackup", "toggleDetails");
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        
        return this.fetchServerWideBackupTasks();
    }

    attached() {
        super.attached();
        
        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchReconnect(() => this.refresh()));

        this.updateUrl(appUrl.forServerWideBackupList());
    }
    
    private refresh() {
        return this.fetchServerWideBackupTasks();
    }

    private fetchServerWideBackupTasks(): JQueryPromise<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult> { 
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
            ...this.serverWideBackupTasks(),
           ] as Array<{ taskId: number }>;
        
        const oldTaskIds = oldTasks.map(x => x.taskId);
        const newTaskIds = result.Tasks.map(x => x.TaskId);
        const toDeleteIds = _.without(oldTaskIds, ...newTaskIds);
        
        this.mergeTasks(this.serverWideBackupTasks,
            result.Tasks, 
            toDeleteIds,
            (dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideBackupTask) => new ongoingTaskServerWideBackupListModel(dto));
    }
      
    private mergeTasks<T extends ongoingTaskListModel>(container: KnockoutObservableArray<ongoingTaskServerWideBackupListModel>,
        incomingData: Array<Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask>,
                                                       toDelete: Array<number>,
        ctr: (dto: Raven.Server.Web.System.AdminStudioServerWideHandler.ServerWideTasksResult.ServerWideTask) => ongoingTaskServerWideBackupListModel) {
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

    confirmEnableServerWideBackup(model: ongoingTaskModel) {      
        this.confirmationMessage("Enable Task", "You're enabling the server-wide backup task: " + model.taskName(), {
            buttons: ["Cancel", "Enable"]
        })
            .done(result => {
                if (result.can) {
                    new toggleServerWideTaskCommand("Backup", model.taskName(), false)
                        .execute()
                        .done(() => model.taskState('Enabled'))
                        .always(() => this.fetchServerWideBackupTasks());
                }
            });
    }

    confirmDisableServerWideBackup(model: ongoingTaskModel) {
        this.confirmationMessage("Disable Task", "You're disabling the server-wide backup task: " + model.taskName(), {
            buttons: ["Cancel", "Disable"]
        })
            .done(result => {
                if (result.can) {
                    new toggleServerWideTaskCommand("Backup", model.taskName(), true)
                        .execute()
                        .done(() => model.taskState('Disabled'))
                        .always(() => this.fetchServerWideBackupTasks()); 
                }
            });
    }

    confirmRemoveServerWideBackup(model: ongoingTaskModel) {
        this.confirmationMessage("Delete Task", "You're deleting the server-wide backup task: " + model.taskName(), {
            buttons: ["Cancel", "Delete"]
        })
            .done(result => {
                if (result.can) {
                    this.deleteServerWideBackupTask(model.taskName());
                }
            });
    }

    private deleteServerWideBackupTask(taskName: string) {
        new deleteServerWideTaskCommand("Backup", taskName)
            .execute()
            .done(() => this.fetchServerWideBackupTasks()); 
    }

    addNewServerWideBackupTask() {
        router.navigate(appUrl.forEditServerWideBackup()); 
    }
}

export = serverWideBackupList;
