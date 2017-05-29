import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteDatabaseConfirm = require("viewmodels/resources/deleteDatabaseConfirm");
import databaseInfo = require("models/resources/info/databaseInfo");
import messagePublisher = require("common/messagePublisher");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplication");
import ongoingTaskBackup = require("models/database/tasks/ongoingTaskBackup");
import ongoingTaskEtl = require("models/database/tasks/ongoingTaskETL");
import ongoingTaskSql = require("models/database/tasks/ongoingTaskSQL");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import ongoingTask = require("models/database/tasks/ongoingTask");
import createOngoingTask = require("viewmodels/database/tasks/createOngoingTask");

type TasksNamesInUI = "External Replication" | "RavenDB ETL" | "SQL ETL" | "Backup" | "Subscription";

class ongoingTasks extends viewModelBase {
    
    // Todo: Get info for db group topology ! members & promotable list..

    clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();

    // Ongoing tasks lists:
    replicationTasks = ko.observableArray<ongoingTaskReplication>(); 
    etlTasks = ko.observableArray<ongoingTaskEtl>();
    sqlTasks = ko.observableArray<ongoingTaskSql>();
    backupTasks = ko.observableArray<ongoingTaskBackup>();

    existingTasksArray = ko.observableArray<string>(); // Used in the Filter by Type drop down
    private existingTasksSet = new Set<TasksNamesInUI>();
    selectedTaskType = ko.observable<string>();
    
    subscriptionsCount = ko.observable<number>();
    subsCountText: KnockoutComputed<string>;
    urlForSubscriptions: KnockoutComputed<string>;
    
    constructor() {
        super();

        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.nodeTag());
        this.subsCountText = ko.pureComputed(() => { return `(${this.subscriptionsCount()})`; });
        this.urlForSubscriptions = ko.pureComputed(() => appUrl.forSubscriptions(this.activeDatabase()));
        this.selectedTaskType("All");
    }

    activate(args: any): JQueryPromise < Raven.Server.Web.System.OngoingTasksResult> {
        super.activate(args);
        return this.fetchOngoingTasks();
    }

    attached() {
        super.attached();
        
        const db = this.activeDatabase();
        this.updateUrl(appUrl.forOngoingTasks(db));
    }

    private fetchOngoingTasks(): JQueryPromise<Raven.Server.Web.System.OngoingTasksResult> {
        const db = this.activeDatabase();
        return new ongoingTasksCommand(db)
            .execute()
            .done((info) => {
                return this.processTasksResult(info);
            });
    }

    private processTasksResult(result: Raven.Server.Web.System.OngoingTasksResult) { 
        this.subscriptionsCount(result.SubscriptionsCount);
        if (result.SubscriptionsCount > 0) {
            this.existingTasksSet.add("Subscription");
        }

        // Init viewModel tasks lists:
        result.OngoingTasksList.map((task) => {
            switch (task.TaskType) {
                case 'Replication':
                    this.replicationTasks().push(new ongoingTaskReplication(task as Raven.Server.Web.System.OngoingTaskReplication));
                    this.existingTasksSet.add("External Replication");
                    break;
                case 'Backup':
                    this.backupTasks().push(new ongoingTaskBackup(task as Raven.Server.Web.System.OngoingTaskBackup));
                    this.existingTasksSet.add("Backup");
                    break;
                case 'ETL':
                    this.etlTasks().push(new ongoingTaskEtl(task as Raven.Server.Web.System.OngoingTaskETL));
                    this.existingTasksSet.add("RavenDB ETL");
                    break;
                case 'SQL':
                    this.sqlTasks().push(new ongoingTaskSql(task as Raven.Server.Web.System.OngoingTaskSQL));
                    this.existingTasksSet.add("SQL ETL");
                    break;
            };
        });

        this.existingTasksArray(Array.from(this.existingTasksSet).sort());
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    removeOngoingTask(db: databaseInfo) {
       // ...
    }

    private removeTaskConfirm(toDelete: databaseInfo[]) {
        const confirmDeleteViewModel = new deleteDatabaseConfirm(toDelete);

        confirmDeleteViewModel
            .result
            .done((confirmResult: deleteDatabaseConfirmResult) => {
                if (confirmResult.can) {
                    // ...
                }
            });

        app.showBootstrapDialog(confirmDeleteViewModel);
    }

    private onTaskRemoved() {
        // ...
    }

    private removeTask(taskInfo: ongoingTask) {
        // ...
        messagePublisher.reportSuccess(`Task ${taskInfo.taskType} was successfully removed`);
    }

    addNewOngoingTask() {
        this.newOngoingTask();
    }

    newOngoingTask() {
        const addOngoingTaskView = new createOngoingTask();
        app.showBootstrapDialog(addOngoingTaskView);
    }

    editOngoingTask() {
        // ....
    }

    setSelectedTaskType(taskName: string) {
        this.selectedTaskType(taskName);
    }
}

export = ongoingTasks;