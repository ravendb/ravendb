import appUrl = require("common/appUrl");
import router = require("plugins/router");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import ongoingTaskReplicationHubDefinitionListModel = require("models/database/tasks/ongoingTaskReplicationHubDefinitionListModel");
import ongoingTaskBackupListModel = require("models/database/tasks/ongoingTaskBackupListModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import toggleOngoingTaskCommand = require("commands/database/tasks/toggleOngoingTaskCommand");
import databaseGroupGraph = require("models/database/dbGroup/databaseGroupGraph");
import getDatabaseCommand = require("commands/resources/getDatabaseCommand");
import ongoingTaskListModel = require("models/database/tasks/ongoingTaskListModel");
import manualBackupListModel = require("models/database/tasks/manualBackupListModel");
import accessManager = require("common/shell/accessManager");
import getManualBackupCommand = require("commands/database/tasks/getManualBackupCommand");

class backups extends viewModelBase {
    
    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();
    
    periodicBackupTasks = ko.observableArray<ongoingTaskBackupListModel>();
    recentManualBackup = ko.observable<manualBackupListModel>();
    
    isManualBackupInProgress = ko.observable<boolean>(false); // todo !!! create issue... for server ep

    canNavigateToServerWideBackupTasks: KnockoutComputed<boolean>;
    serverWideBackupUrl: string;
    ongoingTasksUrl: string;

    private graph = new databaseGroupGraph();
    backupsOnly = true; // used in graph legend

    private watchedBackups = new Map<number, number>();
    
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveOngoingTask", "confirmEnableOngoingTask", "confirmDisableOngoingTask",
                                   "toggleDetails", "createNewPeriodicBackupTask");
        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.localNodeTag());
        this.serverWideBackupUrl = appUrl.forServerWideBackupList();
        this.ongoingTasksUrl = appUrl.forOngoingTasks(this.activeDatabase());
        this.canNavigateToServerWideBackupTasks = accessManager.default.clusterAdminOrClusterNode;
    }

    activate(args: any): JQueryPromise<any> {
        super.activate(args);
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks(), this.fetchManualBackup());
    }

    attached() {
        super.attached();

        this.addNotification(this.changesContext.serverNotifications()
            .watchClusterTopologyChanges(() => this.refresh()));
        
        this.addNotification(this.changesContext.serverNotifications()
            .watchDatabaseChange(this.activeDatabase().name, () => this.refresh()));
        
        this.addNotification(this.changesContext.serverNotifications().watchReconnect(() => this.refresh()));
        
        this.updateUrl(appUrl.forBackups(this.activeDatabase()));
    }

    compositionComplete(): void {
        super.compositionComplete();

        this.registerDisposableHandler($(document), "fullscreenchange", () => {
            $("body").toggleClass("fullscreen", $(document).fullScreen());
            this.graph.onResize();
        });
        
        this.graph.init($("#databaseGroupGraphContainer"));
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
        return $.when<any>(this.fetchDatabaseInfo(), this.fetchOngoingTasks(), this.fetchManualBackup());
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

    private fetchManualBackup(): JQueryPromise<Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult> {
        const db = this.activeDatabase().name;
        return new getManualBackupCommand(db)
            .execute()
            .done((manualBackupInfo) => {
                this.processManualBackupResult(manualBackupInfo);
            });
    }

    refreshManualBackupInfo() {
        this.fetchManualBackup();
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
    
    toggleDetails(item: ongoingTaskListModel) {
        item.toggleDetails();
    }
    
    private processManualBackupResult(dto: Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult) {
       this.recentManualBackup(dto.Status ? new manualBackupListModel(dto.Status) : null);
    }
    
    private processTasksResult(result: Raven.Server.Web.System.OngoingTasksResult) {
        const oldTasks = [
            ...this.periodicBackupTasks()
            ] as Array<{ taskId: number }>;

        const oldTaskIds = oldTasks.map(x => x.taskId);
        
        const newTaskIds = result.OngoingTasksList.map(x => x.TaskId);
        newTaskIds.push(...result.PullReplications.map(x => x.TaskId));

        const toDeleteIds = _.without(oldTaskIds, ...newTaskIds);

        const groupedTasks = _.groupBy(result.OngoingTasksList, x => x.TaskType);
       
        this.mergeTasks(this.periodicBackupTasks, 
            groupedTasks['Backup' as Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType], 
            toDeleteIds,
            (dto: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup) => new ongoingTaskBackupListModel(dto, task => this.watchBackupCompletion(task)));
        
        // Sort backup tasks 
        const groupedBackupTasks = _.groupBy(this.periodicBackupTasks(), x => x.isServerWide());
        const serverWideBackupTasks = groupedBackupTasks.true;
        const ongoingBackupTasks = groupedBackupTasks.false;

        if (ongoingBackupTasks) {
            this.periodicBackupTasks(serverWideBackupTasks ? ongoingBackupTasks.concat(serverWideBackupTasks) : ongoingBackupTasks);            
        } else if (serverWideBackupTasks) {
            this.periodicBackupTasks(serverWideBackupTasks);
        }
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
        });
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
        
        this.confirmationMessage("Delete Task", "You're deleting task of type: " + model.taskType(), {
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
    
    createNewPeriodicBackupTask() {
        const url = appUrl.forEditPeriodicBackupTask(this.activeDatabase());
        router.navigate(url); 
    }

    createManualBackup() {
        const url = appUrl.forEditManualBackup(this.activeDatabase());
        router.navigate(url);
    }

    navigateToRestoreDatabase() {
        const url = appUrl.forDatabases("restore");
        router.navigate(url);
    }
}

export = backups;
