import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplication>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
    private taskId: number = null;

    activate(args: any) { 
        super.activate(args);

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            new ongoingTaskInfoCommand(this.activeDatabase(), "Replication", this.taskId)
                .execute()
                .done((result: Raven.Client.Server.Operations.GetTaskInfoResult) => this.editedExternalReplication(new ongoingTaskReplication(result)))
                .fail(() => router.navigate(appUrl.forOngoingTasks(this.activeDatabase())));
        }
        else {
            // 2. Creating a new task
            this.isAddingNewReplicationTask(true);
            this.editedExternalReplication(ongoingTaskReplication.empty());
        }
    }

    compositionComplete() {
        document.getElementById('taskName').focus();
    }

    saveExternalReplication() {
        // 1. validate model
        if (!this.validate()) {
             return;
        }

        // 2. create/add the new replication task
        const dto = this.editedExternalReplication().toDto();

        this.taskId = this.isAddingNewReplicationTask() ? 0 : this.taskId;

        new saveExternalReplicationTaskCommand(this.activeDatabase(), this.taskId, dto)
            .execute()
            .done(() => this.goToOngoingTasksView());
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedExternalReplication().validationGroup))
            valid = false;

        return valid;
    }
}

export = editExternalReplicationTask;