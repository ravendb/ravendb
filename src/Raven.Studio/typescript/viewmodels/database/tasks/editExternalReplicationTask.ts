import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplication>();
    isAddingNewReplicationTask = ko.observable<boolean>(true);
    private taskId: number = null;

    activate(args: any) { 
        super.activate(args);

        if (args.taskId) {
            this.isAddingNewReplicationTask(false);
            this.taskId = args.taskId;

            //new ongoingTaskInfoCommand(this.activeDatabase(), args.taskType, this.taskId)
            //    .execute()
            //    .done((result) => {
            //        this.editedExternalReplication(result);
            //    .fail(() => { ... })
            //    });

            //TODO: fetch data from server
        }
        else {
            this.isAddingNewReplicationTask(true);

            // No task id in url, init an empty model ==> create action
            this.editedExternalReplication(ongoingTaskReplication.empty());
        }
    }
    
    saveExternalReplication() {
        // 1. validate model
        if (!this.validate()) {
             return;
        }

        // 2. create/add the new replication task
        const dto = this.editedExternalReplication().toDto();

        this.taskId = this.isAddingNewReplicationTask() ? 0 : this.taskId;

        new saveExternalReplicationTaskCommand(dto, this.activeDatabase(), this.taskId)
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