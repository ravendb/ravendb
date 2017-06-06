import app = require("durandal/app");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import database = require("models/resources/database");
import databaseInfo = require("models/resources/info/databaseInfo");
import ongoingTasksCommand = require("commands/database/tasks/getOngoingTasksCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");
import ongoingTaskBackup = require("models/database/tasks/ongoingTaskBackupModel");
import ongoingTaskEtl = require("models/database/tasks/ongoingTaskETLModel");
import ongoingTaskSql = require("models/database/tasks/ongoingTaskSQLModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import createOngoingTask = require("viewmodels/database/tasks/createOngoingTask");
import deleteOngoingTaskConfirm = require("viewmodels/database/tasks/deleteOngoingTaskConfirm");
import enableOngoingTaskConfirm = require("viewmodels/database/tasks/enableOngoingTaskConfirm");
import disableOngoingTaskConfirm = require("viewmodels/database/tasks/disableOngoingTaskConfirm");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import messagePublisher = require("common/messagePublisher");
import disableOngoingTaskCommand = require("commands/database/tasks/disableOngoingTaskCommand");

type TasksNamesInUI = "External Replication" | "RavenDB ETL" | "SQL ETL" | "Backup" | "Subscription";

class ongoingTasks extends viewModelBase {
    
    // Todo: Get info for db group topology ! members & promotable list..

    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();

    // Ongoing tasks lists:
    replicationTasks = ko.observableArray<ongoingTaskReplication>(); 
    etlTasks = ko.observableArray<ongoingTaskEtl>();
    sqlTasks = ko.observableArray<ongoingTaskSql>();
    backupTasks = ko.observableArray<ongoingTaskBackup>();

    existingTaskTypes = ko.observableArray<string>();
    selectedTaskType = ko.observable<string>();
    
    subscriptionsCount = ko.observable<number>();
    subsCountText: KnockoutComputed<string>;
    urlForSubscriptions: KnockoutComputed<string>;
    
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveOngoingTask", "confirmEnableOngoingTask", "confirmDisableOngoingTask");

        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.nodeTag());
        this.subsCountText = ko.pureComputed(() => { return `(${this.subscriptionsCount()})`; });
        this.urlForSubscriptions = ko.pureComputed(() => appUrl.forSubscriptions(this.activeDatabase()));
    }

    activate(args: any): JQueryPromise < Raven.Server.Web.System.OngoingTasksResult> {
        super.activate(args);
        return this.fetchOngoingTasks();
    }

    attached() {
        super.attached();
        
        const db = this.activeDatabase();
        this.updateUrl(appUrl.forOngoingTasks(db));

        this.selectedTaskType(_.first(this.existingTaskTypes()) || "All");
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
        this.replicationTasks([]);
        this.backupTasks([]);
        this.etlTasks([]);
        this.sqlTasks([]);

        const taskTypesSet = new Set<TasksNamesInUI>();

        this.subscriptionsCount(result.SubscriptionsCount);
        if (result.SubscriptionsCount > 0) {
            taskTypesSet.add("Subscription");
        }

        result.OngoingTasksList.map((task) => {
            switch (task.TaskType) {
                case 'Replication':
                    this.replicationTasks.push(new ongoingTaskReplication(task as Raven.Server.Web.System.OngoingTaskReplication));
                    taskTypesSet.add("External Replication");
                    break;
                case 'Backup':
                    this.backupTasks.push(new ongoingTaskBackup(task as Raven.Server.Web.System.OngoingTaskBackup));
                    taskTypesSet.add("Backup");
                    break;
                case 'RavenEtl':
                    this.etlTasks.push(new ongoingTaskEtl(task as Raven.Server.Web.System.OngoingRavenEtl));
                    taskTypesSet.add("RavenDB ETL");
                    break;
                case 'SqlEtl':
                    this.sqlTasks.push(new ongoingTaskSql(task as Raven.Server.Web.System.OngoingSqlEtl));
                    taskTypesSet.add("SQL ETL");
                    break;
            };
        });

        this.existingTaskTypes(Array.from(taskTypesSet).sort());
    }

    manageDatabaseGroupUrl(dbInfo: databaseInfo): string {
        return appUrl.forManageDatabaseGroup(dbInfo);
    }

    confirmEnableOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        const confirmEnableViewModel = new enableOngoingTaskConfirm(db, model.taskType(), model.taskId); 
        app.showBootstrapDialog(confirmEnableViewModel);
        confirmEnableViewModel.result.done(result => {
            if (result.can) {
                new disableOngoingTaskCommand(db, model.taskType(), model.taskId, false)
                    .execute()
                    .done(() => model.taskState('Disabled'))
                    .always(() => this.fetchOngoingTasks());
            }
        });
    }

    confirmDisableOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        const confirmDisableViewModel = new disableOngoingTaskConfirm(db, model.taskType(), model.taskId);
        app.showBootstrapDialog(confirmDisableViewModel);
        confirmDisableViewModel.result.done(result => {
            if (result.can) {
                new disableOngoingTaskCommand(db, model.taskType(), model.taskId, true)
                    .execute()
                    .done(() => model.taskState('Enabled'))
                    .always(() => this.fetchOngoingTasks());
            }
        });
    }

    confirmRemoveOngoingTask(model: ongoingTaskModel) {
        const db = this.activeDatabase();

        const confirmDeleteViewModel = new deleteOngoingTaskConfirm(db, model.taskType(), model.taskId);
        app.showBootstrapDialog(confirmDeleteViewModel);
        confirmDeleteViewModel.result.done(result => {
            if (result.can) {
                this.deleteOngoingTask(db, model);
            }
        });
    }

    private deleteOngoingTask(db: database, model: ongoingTaskModel) {
        new deleteOngoingTaskCommand(db, model.taskType(), model.taskId)
            .execute()
            .done(() => {
                messagePublisher.reportSuccess("Successfully deleted " + model.taskType() + " task");
                this.fetchOngoingTasks();
            })
            .fail(() => {
                messagePublisher.reportError("Failed to delete " + model.taskType() + " task");
            });
    }

    addNewOngoingTask() {
        const addOngoingTaskView = new createOngoingTask();
        app.showBootstrapDialog(addOngoingTaskView);
    }

    setSelectedTaskType(taskName: string) {
        this.selectedTaskType(taskName);
    }
}

export = ongoingTasks;