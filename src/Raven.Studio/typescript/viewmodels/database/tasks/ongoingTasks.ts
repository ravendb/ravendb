import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import databaseInfo = require("models/resources/info/databaseInfo");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import ongoingTaskReplicationListModel = require("models/database/tasks/ongoingTaskReplicationListModel");
import ongoingTaskReplicationHubDefinitionListModel = require("models/database/tasks/ongoingTaskReplicationHubDefinitionListModel");
import ongoingTaskBackupListModel = require("models/database/tasks/ongoingTaskBackupListModel");
import ongoingTaskRavenEtlListModel = require("models/database/tasks/ongoingTaskRavenEtlListModel");
import ongoingTaskSqlEtlListModel = require("models/database/tasks/ongoingTaskSqlEtlListModel");
import ongoingTaskSubscriptionListModel = require("models/database/tasks/ongoingTaskSubscriptionListModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import createOngoingTask = require("viewmodels/database/tasks/createOngoingTask");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import toggleOngoingTaskCommand = require("commands/database/tasks/toggleOngoingTaskCommand");
import etlProgressCommand = require("commands/database/tasks/etlProgressCommand");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import etlScriptDefinitionCache = require("models/database/stats/etlScriptDefinitionCache");
import ongoingTaskReplicationSinkListModel = require("models/database/tasks/ongoingTaskReplicationSinkListModel");
import accessManager = require("common/shell/accessManager");
import generalUtils = require("common/generalUtils");

class ongoingTasks extends viewModelBase {
    
    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();

    private graph = new databaseGroupGraph();
    
    private watchedBackups = new Map<number, number>();
    private etlProgressWatch: number;

    private definitionsCache: etlScriptDefinitionCache;

    // The Ongoing Tasks Lists:
    replicationTasks = ko.observableArray<ongoingTaskReplicationListModel>(); 
    etlTasks = ko.observableArray<ongoingTaskRavenEtlListModel>();
    sqlTasks = ko.observableArray<ongoingTaskSqlEtlListModel>();
    backupTasks = ko.observableArray<ongoingTaskBackupListModel>();
    subscriptionTasks = ko.observableArray<ongoingTaskSubscriptionListModel>();
    pullReplicationHubTasks = ko.observableArray<ongoingTaskReplicationHubDefinitionListModel>();
    pullReplicationSinkTasks = ko.observableArray<ongoingTaskReplicationSinkListModel>();
    
    showReplicationSection = this.createShowSectionComputed(this.replicationTasks, 'External Replication');
    showEtlSection = this.createShowSectionComputed(this.etlTasks, 'RavenDB ETL');
    showSqlSection = this.createShowSectionComputed(this.sqlTasks, 'SQL ETL');
    showBackupSection = this.createShowSectionComputed(this.backupTasks, 'Backup');
    showSubscriptionsSection = this.createShowSectionComputed(this.subscriptionTasks, 'Subscription');
    showPullReplicationHubSection = this.createShowSectionComputedForPullHub(this.pullReplicationHubTasks);
    showPullReplicationSinkSection = this.createShowSectionComputed(this.pullReplicationSinkTasks, "Replication Sink");

    tasksTypesOrderForUI = ["Replication", "RavenEtl", "SqlEtl", "Backup", "Subscription", "PullReplicationAsHub", "PullReplicationAsSink"] as Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType>;
    existingTaskTypes = ko.observableArray<TasksNamesInUI | "All tasks">();    
    selectedTaskType = ko.observable<TasksNamesInUI | "All tasks">();
    
    taskNameToCount: KnockoutComputed<dictionary<number>>;    
    
    existingNodes = ko.observableArray<string>();
    selectedNode = ko.observable<string>();

    canNavigateToServerWideBackupTasks: KnockoutComputed<boolean>;
    serverWideBackupUrl: string;
    
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveOngoingTask", "confirmEnableOngoingTask", "confirmDisableOngoingTask", "toggleDetails", "showItemPreview");

        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.localNodeTag());
        this.serverWideBackupUrl = appUrl.forServerWideBackupList();
        this.canNavigateToServerWideBackupTasks = accessManager.default.clusterAdminOrClusterNode;
        this.taskNameToCount = ko.pureComputed<dictionary<number>>(() => {
            return {
                "External Replication": this.replicationTasks().length,
                "RavenDB ETL": this.etlTasks().length,
                "SQL ETL": this.sqlTasks().length,
                "Backup": this.backupTasks().length,
                "Subscription": this.subscriptionTasks().length,
                "Pull Replication Hub": this.pullReplicationHubTasks().length,
                "Pull Replication Sink": this.pullReplicationSinkTasks().length
            }
        });
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks());
    }

    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.activeDatabase().name, () => this.refresh()));
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
        
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

        this.definitionsCache = new etlScriptDefinitionCache(this.activeDatabase());
        
        this.graph.init($("#databaseGroupGraphContainer"));
    }
    
    private fetchEtlProcess() {
        return new etlProgressCommand(this.activeDatabase())
            .execute()
            .done(results => {
                results.Results.forEach(taskProgress => {
                    switch (taskProgress.EtlType) {
                        case "Sql":
                            const matchingSqlTask = this.sqlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingSqlTask) {
                                matchingSqlTask.updateProgress(taskProgress);
                            }
                            break;
                        case "Raven":
                            const matchingEtlTask = this.etlTasks().find(x => x.taskName() === taskProgress.TaskName);
                            if (matchingEtlTask) {
                                matchingEtlTask.updateProgress(taskProgress);    
                            }
                            break;
                    }
                });
                
                // tasks w/o defined connection string won't get progress update - update them manually 
                
                this.sqlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });
                
                this.etlTasks().forEach(task => {
                    if (task.loadingProgress()) {
                        task.loadingProgress(false);
                    }
                });
            });
    }
    
    private createShowSectionComputed(tasksContainer: KnockoutObservableArray<{ responsibleNode: KnockoutObservable<Raven.Client.ServerWide.Operations.NodeId> }>, taskType: TasksNamesInUI) {
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

    private createShowSectionComputedForPullHub(tasksContainer: KnockoutObservableArray<ongoingTaskReplicationHubDefinitionListModel>) {
        return ko.pureComputed(() =>  {
            const hasAnyTask = tasksContainer().length > 0;
            const matchesSelectTaskType = this.selectedTaskType() === "Replication Hub" || this.selectedTaskType() === "All tasks";

            let nodeMatch = true;
            if (this.selectedNode() !== "All nodes") {
                nodeMatch = _.some(tasksContainer(), 
                               x => _.some(x.ongoingHubs(),
                                    task => task.responsibleNode() && task.responsibleNode().NodeTag === this.selectedNode()));
            }

            return hasAnyTask && matchesSelectTaskType && nodeMatch;
        });
    }

    createResponsibleNodeUrl(task: ongoingTaskListModel) {
        return ko.pureComputed(() => {
            const node = task.responsibleNode();
            const db = this.activeDatabase();
            
            if (node && db) {
                return node.NodeUrl + appUrl.forOngoingTasks(db);
            }
            
            return "#";
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
    
    private watchEtlProgress() {
        if (!this.etlProgressWatch) {
            this.fetchEtlProcess();
            
            let intervalId = setInterval(() => {
                this.fetchEtlProcess();
            }, 3000);
            
            this.etlProgressWatch = intervalId;
            
            this.registerDisposable({
                dispose: () => {
                    if (intervalId) {
                        clearInterval(intervalId);
                        intervalId = 0;
                        this.etlProgressWatch = null;
                    }
                }
            })
        }
    }
    
    toggleDetails(item: ongoingTaskListModel) {
        item.toggleDetails();
        
        const isEtl = item.taskType() === "RavenEtl" || item.taskType() === "SqlEtl";
        if (item.showDetails() && isEtl) {
            this.watchEtlProgress();
        }
    }
    
    private processTasksResult(result: Raven.Server.Web.System.OngoingTasksResult) {
        const oldTasks = [
            ...this.replicationTasks(),
            ...this.backupTasks(),
            ...this.etlTasks(),
            ...this.sqlTasks(),
            ...this.pullReplicationSinkTasks(),
            ...this.pullReplicationHubTasks(),
            ...this.subscriptionTasks()] as Array<{ taskId: number }>;

        const oldTaskIds = oldTasks.map(x => x.taskId);
        
        const newTaskIds = result.OngoingTasksList.map(x => x.TaskId);
        newTaskIds.push(...result.PullReplications.map(x => x.TaskId));

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
        
        // Sort backup tasks 
        const groupedBackupTasks = _.groupBy(this.backupTasks(), x => x.isServerWide());
        const serverWideBackupTasks = groupedBackupTasks.true;
        const ongoingBackupTasks = groupedBackupTasks.false;

        if (ongoingBackupTasks) {
            this.backupTasks(serverWideBackupTasks ? ongoingBackupTasks.concat(serverWideBackupTasks) : ongoingBackupTasks);            
        } else if (serverWideBackupTasks) {
            this.backupTasks(serverWideBackupTasks);
        }
        
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
        this.mergeTasks(this.pullReplicationSinkTasks,
            groupedTasks['PullReplicationAsSink' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink) => new ongoingTaskReplicationSinkListModel(dto));
        
        const hubOngoingTasks = groupedTasks['PullReplicationAsHub' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType] as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub[];
        this.mergePullReplicationHubs(result.PullReplications, hubOngoingTasks || [], toDeleteIds);
        
        const taskTypes = Object.keys(groupedTasks); 
        if ((hubOngoingTasks || []).length === 0 && result.PullReplications.length) {
            // we have any pull replication definitions but no incoming connections, so append PullReplicationAsHub task type
            taskTypes.push("PullReplicationAsHub" as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType);
        }

        this.existingTaskTypes(this.tasksTypesOrderForUI.filter(x => _.includes(taskTypes, x))
            .map(ongoingTaskModel.mapTaskType));
        
        this.existingNodes(_.uniq(result
            .OngoingTasksList
            .map(x => x.ResponsibleNode.NodeTag)
            .filter(x => x))
            .sort());
    }
    
   
     
    private mergePullReplicationHubs(incomingDefinitions: Array<Raven.Client.Documents.Operations.Replication.PullReplicationDefinition>,
                                     incomingData: Array<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub>,
                                     toDelete: Array<number>) {
        
        const container = this.pullReplicationHubTasks;
        
        // remove old hub tasks
        container()
            .filter(x => _.includes(toDelete, x.taskId))
            .forEach(task => container.remove(task));
     
        (incomingDefinitions || []).forEach(item => {
            const existingItem = container().find(x => x.taskId === item.TaskId);
            if (existingItem) {
                existingItem.update(item);
                existingItem.updateChildren(incomingData.filter(x => x.TaskId === item.TaskId));
            } else {
                const newItem = new ongoingTaskReplicationHubDefinitionListModel(item);
                const insertIdx = _.sortedIndexBy(container(), newItem, x => x.taskName().toLocaleLowerCase());
                container.splice(insertIdx, 0, newItem);
                newItem.updateChildren(incomingData.filter(x => x.TaskId === item.TaskId));
            }
        });
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
                const insertIdx = generalUtils.sortedAlphaNumericIndex(container(), newItem, x => x.taskName().toLocaleLowerCase());
                container.splice(insertIdx, 0, newItem);
            }
        });
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    confirmEnableOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        this.confirmationMessage("Enable Task", "You're enabling task of type: " + model.taskType(), {
            buttons: ["Cancel", "Enable"]
        })
            .done(result => {
                if (result.can) {
                    new toggleOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName(), false)
                        .execute()
                        .done(() => model.taskState('Enabled'))
                        .always(() => this.fetchOngoingTasks());
                }
            });
    }

    confirmDisableOngoingTask(model: ongoingTaskModel | ongoingTaskReplicationHubDefinitionListModel) {
        const db = this.activeDatabase();

        this.confirmationMessage("Disable Task", "You're disabling task of type: " + model.taskType(), {
            buttons: ["Cancel", "Disable"]
        })
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
        
        const taskType = ongoingTaskModel.mapTaskType(model.taskType());
        
        this.confirmationMessage("Delete Task", "You're deleting " + taskType + ": " + model.taskName(), {
            buttons: ["Cancel", "Delete"]
        })
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

    setSelectedTaskType(taskName: TasksNamesInUI | "All tasks") {
        this.selectedTaskType(taskName);
    }

    setSelectedNode(node: string) {
        this.selectedNode(node);
    }

    showItemPreview(item: ongoingTaskListModel, scriptName: string) {
        const type: Raven.Client.Documents.Operations.ETL.EtlType = item.taskType() === "RavenEtl" ? "Raven" : "Sql";
        this.definitionsCache.showDefinitionFor(type, item.taskId, scriptName);
    }
}

export = ongoingTasks;
