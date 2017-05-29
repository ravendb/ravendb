import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskReplication = require("models/database/tasks/ongoingTaskReplicationModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

class editExternalReplicationTask extends viewModelBase {

    editedExternalReplication = ko.observable<ongoingTaskReplication>();
    isAddingNewRepTask = ko.observable<boolean>();
    private taskId: number;

    constructor() {
        super();
        this.initObservables();
    }

    private initObservables() {
        this.isAddingNewRepTask(true);
        this.taskId = null;
    }

    activate(args: any) { 
        super.activate(args);

        // Init the model

        if (args.taskId) {
            this.isAddingNewRepTask(false);
            this.taskId = args.taskId;

            //new ongoingTaskInfoCommand(this.activeDatabase(), args.taskType, this.taskId)
            //    .execute()
            //    .done((result) => {
            //        this.editedExternalReplication(result);
            //    .fail(() => { ... })
            //    });

            // TODO: call the new ep to get info from server if there is task id in url ==> an edit action
            // Now the following is just simulation....
            alert("Simulating getting data from server...");
            let tempTask = ongoingTaskReplication.Simulation();
            tempTask.taskId = this.taskId;

            this.editedExternalReplication(tempTask);
        }
        else {
            this.isAddingNewRepTask(true);

            // No task id in url, init an empty model ==> create action
            this.editedExternalReplication(ongoingTaskReplication.empty());
        }
    }

    attached() {
        super.attached(); 
    }
    
    saveExternalReplication() {
        // 1. validate model
        if (!this.validate()) {
             return;
        }

        // 2. create/add the new replication task
        const newRepTask: externalReplicationDataFromUI = {
            DestinationURL: this.editedExternalReplication().destinationURL(),
            DestinationDB: this.editedExternalReplication().destinationDB(),
            ApiKey: this.editedExternalReplication().apiKey()
        };

        return new saveExternalReplicationTaskCommand(newRepTask, this.activeDatabase(), this.taskId)
            .execute()
            .done(() => {
                this.goToOngoingTasksView();
            });
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    goToOngoingTasksView() {
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