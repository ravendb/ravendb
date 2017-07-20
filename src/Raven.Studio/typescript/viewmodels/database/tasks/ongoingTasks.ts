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
import ongoingTaskSubscription = require("models/database/tasks/ongoingTaskSubscriptionModel");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import createOngoingTask = require("viewmodels/database/tasks/createOngoingTask");
import deleteOngoingTaskConfirm = require("viewmodels/database/tasks/deleteOngoingTaskConfirm");
import enableOngoingTaskConfirm = require("viewmodels/database/tasks/enableOngoingTaskConfirm");
import disableOngoingTaskConfirm = require("viewmodels/database/tasks/disableOngoingTaskConfirm");
import ongoingTaskModel = require("models/database/tasks/ongoingTaskModel");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import toggleOngoingTaskCommand = require("commands/database/tasks/toggleOngoingTaskCommand");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

type TasksNamesInUI = "External Replication" | "RavenDB ETL" | "SQL ETL" | "Backup" | "Subscription";

class ongoingTasks extends viewModelBase {
    
    // Todo: Get info for db group topology ! members & promotable list..

    private clusterManager = clusterTopologyManager.default;
    myNodeTag = ko.observable<string>();

    // The Ongoing Tasks Lists:
    replicationTasks = ko.observableArray<ongoingTaskReplication>(); 
    etlTasks = ko.observableArray<ongoingTaskEtl>();
    sqlTasks = ko.observableArray<ongoingTaskSql>();
    backupTasks = ko.observableArray<ongoingTaskBackup>();
    subscriptionTasks = ko.observableArray<ongoingTaskSubscription>();

    existingTaskTypes = ko.observableArray<string>();
    selectedTaskType = ko.observable<string>();

    existingNodes = ko.observableArray<string>();
    selectedNode = ko.observable<string>();
    
    constructor() {
        super();
        this.bindToCurrentInstance("confirmRemoveOngoingTask", "confirmEnableOngoingTask", "confirmDisableOngoingTask", "refreshOngoingTaskInfo");

        this.initObservables();
    }

    private initObservables() {
        this.myNodeTag(this.clusterManager.localNodeTag());
    }

    activate(args: any): JQueryPromise < Raven.Server.Web.System.OngoingTasksResult> {
        super.activate(args);
        return this.fetchOngoingTasks();
    }

    attached() {
        super.attached();
        
        const db = this.activeDatabase();
        this.updateUrl(appUrl.forOngoingTasks(db));

        this.selectedTaskType("All tasks"); 
        this.selectedNode("All nodes"); 
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
        this.subscriptionTasks([]);

        const taskTypesSet = new Set<TasksNamesInUI>();
        const nodesSet = new Set<string>();
      
        result.OngoingTasksList.map((task) => {

            nodesSet.add(task.ResponsibleNode.NodeTag);

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
                case 'Subscription': 
                    this.subscriptionTasks.push(new ongoingTaskSubscription(task as Raven.Server.Web.System.OngoingTaskSubscription)); 
                    taskTypesSet.add("Subscription");
                    break;
            };
        });

        this.existingTaskTypes(Array.from(taskTypesSet).sort());
        this.existingNodes(Array.from(nodesSet).sort());

        this.replicationTasks(_.sortBy(this.replicationTasks(), x => x.taskName().toUpperCase()));
        this.backupTasks(_.sortBy(this.backupTasks(), x => !x.taskName() ? "" : x.taskName().toUpperCase())); 
        this.etlTasks(_.sortBy(this.etlTasks(), x => x.taskName().toUpperCase())); 
        this.sqlTasks(_.sortBy(this.sqlTasks(), x => x.taskName().toUpperCase())); 
        this.subscriptionTasks(_.sortBy(this.subscriptionTasks(), x => x.taskName().toUpperCase())); 
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
                new toggleOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName(), false)
                    .execute()
                    .done(() => {
                        return model.taskState('Enabled');
                    })
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
                new toggleOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName(), true)
                    .execute()
                    .done(() => {
                        return model.taskState('Disabled');
                    })
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
        new deleteOngoingTaskCommand(db, model.taskType(), model.taskId, model.taskName())
            .execute()
            .done(() => this.fetchOngoingTasks());
    }

    refreshOngoingTaskInfo(model: ongoingTaskModel) {
        new ongoingTaskInfoCommand(this.activeDatabase(), "Subscription", model.taskId, model.taskName())
            .execute()
            .done((result: Raven.Client.Documents.Subscriptions.SubscriptionState) => {
                let subscriptionItem = _.find(this.subscriptionTasks(), x => x.taskName() === result.SubscriptionName);

                subscriptionItem.collection(result.Criteria.Collection); 
                subscriptionItem.timeOfLastClientActivity(result.TimeOfLastClientActivity); 
                subscriptionItem.taskState(result.Disabled ? 'Disabled' : 'Enabled'); 
                // TODO: should 'responsibleNode' be added to subscriptionState class ? Or should we put one refersh button for all tasks and then it won't be needed - to be discussed
            });
    }

    disconnectClientFromSubscription(model: ongoingTaskModel) {
        alert("TBD - Disconnect client from subscription");
        // TODO..
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