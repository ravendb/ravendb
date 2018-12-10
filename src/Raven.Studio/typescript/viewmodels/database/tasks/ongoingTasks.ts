import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import databaseInfo = require("models/resources/info/databaseInfo");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import ongoingTaskReplicationListModel = require("models/database/tasks/ongoingTaskReplicationListModel");
import ongoingTaskBackupListModel = require("models/database/tasks/ongoingTaskBackupListModel");
import ongoingTaskRavenEtlListModel = require("models/database/tasks/ongoingTaskRavenEtlListModel");
import ongoingTaskSqlEtlListModel = require("models/database/tasks/ongoingTaskSqlEtlListModel");
import ongoingTaskSubscriptionListModel = require("models/database/tasks/ongoingTaskSubscriptionListModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import createOngoingTask = require("viewmodels/database/tasks/createOngoingTask");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import toggleOngoingTaskCommand = require("commands/database/tasks/toggleOngoingTaskCommand");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");

type TasksNamesInUI = "External Replication" | "RavenDB ETL" | "SQL ETL" | "Backup" | "Subscription";

class ongoingTasks extends viewModelBase {
    
    // Todo: Get info for db group topology ! members & promotable list..

    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();

    private graph = new databaseGroupGraph();
    
    private watchedBackups = new Map<number, number>();

    // The Ongoing Tasks Lists:
    replicationTasks = ko.observableArray<ongoingTaskReplicationListModel>(); 
    etlTasks = ko.observableArray<ongoingTaskRavenEtlListModel>();
    sqlTasks = ko.observableArray<ongoingTaskSqlEtlListModel>();
    backupTasks = ko.observableArray<ongoingTaskBackupListModel>();
    subscriptionTasks = ko.observableArray<ongoingTaskSubscriptionListModel>();
    
    showReplicationSection = this.createShowSectionComputed(this.replicationTasks, 'External Replication');
    showEtlSection = this.createShowSectionComputed(this.etlTasks, 'RavenDB ETL');
    showSqlSection = this.createShowSectionComputed(this.sqlTasks, 'SQL ETL');
    showBackupSection = this.createShowSectionComputed(this.backupTasks, 'Backup');
    showSubscriptionsSection = this.createShowSectionComputed(this.subscriptionTasks, 'Subscription');

    existingTaskTypes = ko.observableArray<string>();
    selectedTaskType = ko.observable<string>();

    existingNodes = ko.observableArray<string>();
    selectedNode = ko.observable<string>();
    
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveOngoingTask", "confirmEnableOngoingTask", "confirmDisableOngoingTask");

        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.localNodeTag());
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        
        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.activeDatabase().name, () => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));

        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }

    attached() {
        super.attached();
        
        const db = this.activeDatabase();
        this.updateUrl(appUrl.forOngoingTasks(db));

        this.selectedTaskType("All tasks"); 
        this.selectedNode("All nodes"); 
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
            this.graph.onResize();
        });
        
        this.graph.init($("#databaseGroupGraphContainer"));
    }
    
    private createShowSectionComputed(tasksContainer: KnockoutObservableArray<ongoingTaskListModel>, taskType: string) {
        return ko.pureComputed(() =>  {
            const hasAnyTask = tasksContainer().length > 0;
            const matchesSelectTaskType = this.selectedTaskType() === taskType || this.selectedTaskType() === "All tasks";
            
            let nodeMatch = true;
            if (this.selectedNode() !== "All nodes") {
                nodeMatch = !!tasksContainer().find(x => x.responsibleNode() && x.responsibleNode().NodeTag === this.selectedNode());
            }
            
            return hasAnyTask && matchesSelectTaskType && nodeMatch;
        });
    }

    private refresh() {
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }
    
    private fetchDatabaseInfo() {
        return new getDatabaseCommand(this.activeDatabase().name)
            .execute()
            .done(dbInfo => {
                this.graph.onDatabaseInfoChanged(dbInfo);
            });
    }

    private fetchOngoingTasks(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const db = this.activeDatabase();
        return new ongoingTasksCommand(db)
            .execute()
            .done((info) => {
                this.processTasksResult(info);
                this.graph.onTasksChanged(info);
            });
    }
    
    private watchBackupCompletion(task: ongoingTaskBackupListModel) {
        if (!this.watchedBackups.has(task.taskId)) {
            let intervalId = setInterval(() => {
            task.refreshBackupInfo(false)
                .done(result => {
                    if (!result.OnGoingBackup) {
                        clearInterval(intervalId);
                        intervalId = 0;
                        this.watchedBackups.delete(task.taskId);
                    }
                })
            }, 3000);
            this.watchedBackups.set(task.taskId, intervalId);
            
            this.registerDisposable({
                dispose: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = 0;
                        this.watchedBackups.delete(task.taskId);
                    }
                }
            });    
        }
    }
    
    private processTasksResult(result: Raven.Server.Web.System.OngoingTasksResult) {
        const oldTasks = [
            ...this.replicationTasks(),
            ...this.backupTasks(),
            ...this.etlTasks(),
            ...this.sqlTasks(),
            ...this.subscriptionTasks()] as Array<ongoingTaskListModel>;

        const oldTaskIds = oldTasks.map(x => x.taskId);

        const newTaskIds = result.OngoingTasksList.map(x => x.TaskId);

        const toDeleteIds = _.without(oldTaskIds, ...newTaskIds);

        const groupedTasks = _.groupBy(result.OngoingTasksList, x => x.TaskType);

        this.mergeTasks(this.replicationTasks, 
            groupedTasks['Replication' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication) => new ongoingTaskReplicationListModel(dto));
        this.mergeTasks(this.backupTasks, 
            groupedTasks['Backup' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => new ongoingTaskBackupListModel(dto, task => this.watchBackupCompletion(task)));
        this.mergeTasks(this.etlTasks, 
            groupedTasks['RavenEtl' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView) => new ongoingTaskRavenEtlListModel(dto));
        this.mergeTasks(this.sqlTasks, 
            groupedTasks['SqlEtl' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView) => new ongoingTaskSqlEtlListModel(dto));
        this.mergeTasks(this.subscriptionTasks, 
            groupedTasks['Subscription' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds, 
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription) => new ongoingTaskSubscriptionListModel(dto));

        this.existingTaskTypes(Object.keys(groupedTasks)
            .sort()
            .map((taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType) => {
                switch (taskType) {
                    case "RavenEtl":
                        return "RavenDB ETL";
                    case "Replication":
                        return "External Replication";
                    case "SqlEtl":
                        return "SQL ETL";
                    default:
                        return taskType;
                }
            }));
        
        this.existingNodes(_.uniq(result
            .OngoingTasksList
            .map(x => x.ResponsibleNode.NodeTag)
            .filter(x => x))
            .sort());
    }
    
     private mergeTasks<T extends ongoingTaskListModel>(container: KnockoutObservableArray<T>, 
                                                       incomingData: Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTask>, 
                                                       toDelete: Array<number>,
                                                       ctr: (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask) => T) {
        // remove old tasks
        container()
            .filter(x => _.includes(toDelete, x.taskId))
            .forEach(task => container.remove(task));
        
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

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    confirmEnableOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        this.confirmationMessage("Enable Task", "You're enabling task of type: " + model.taskType(), ["Cancel", "Enable"])
            .done(result => {
                if (result.can) {
                    new toggleOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName(), false)
                        .execute()
                        .done(() => model.taskState('Enabled'))
                        .always(() => this.fetchOngoingTasks());
                }
            });
    }

    confirmDisableOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        this.confirmationMessage("Disable Task", "You're disabling task of type: " + model.taskType(), ["Cancel", "Disable"])
            .done(result => {
                if (result.can) {
                    new toggleOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName(), true)
                        .execute()
                        .done(() => model.taskState('Disabled'))
                        .always(() => this.fetchOngoingTasks());
                }
            });
    }

    confirmRemoveOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        this.confirmationMessage("Delete Task", "You're deleting task of type: " + model.taskType(), ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    this.deleteOngoingTask(db, model);
                }
            });
    }

    private deleteOngoingTask(db: database, model: ongoingTaskModel) {
        new deleteOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName())
            .execute()
            .done(() => this.fetchOngoingTasks());
    }

    addNewOngoingTask() {
        const addOngoingTaskView = new createOngoingTask();
        app.showBootstrapDialog(addOngoingTaskView);
    }

    setSelectedTaskType(taskName: string) {
        this.selectedTaskType(taskName);
    }

    setSelectedNode(node: string) {
        this.selectedNode(node);
    }

}

export = ongoingTasks;
