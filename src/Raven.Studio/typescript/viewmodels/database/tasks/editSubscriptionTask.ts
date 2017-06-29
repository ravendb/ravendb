import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import router = require("plugins/router");
import saveExternalReplicationTaskCommand = require("commands/database/tasks/saveExternalReplicationTaskCommand");
import ongoingTaskSubscription = require("models/database/tasks/ongoingTaskSubscriptionModel");
import ongoingTaskInfoCommand = require("commands/database/tasks/getOngoingTaskInfoCommand");

class editSubscriptionTask extends viewModelBase {

    editedSubscription = ko.observable<ongoingTaskSubscription>();
    isAddingNewSubscriptionTask = ko.observable<boolean>(true);
    private taskId: number = null;

    activate(args: any) { 
        super.activate(args);

        if (args.taskId) {
            // 1. Editing an existing task
            this.isAddingNewSubscriptionTask(false);
            this.taskId = args.taskId;

            // TODO...
            //new ongoingTaskInfoCommand(this.activeDatabase(), "Subscription", this.taskId)
            //    .execute()
            //    .done((result: Raven.Client.Server.Operations.GetTaskInfoResult) => this.editedSubscription(new ongoingTaskSubscription(result)))
            //    .fail(() => router.navigate(appUrl.forOngoingTasks(this.activeDatabase())));
            this.editedSubscription(ongoingTaskSubscription.empty()); // just for now...
        }
        else {
            // 2. Creating a new task
            this.isAddingNewSubscriptionTask(true);
            this.editedSubscription(ongoingTaskSubscription.empty());
        }
    }

    compositionComplete() {
        document.getElementById('taskName').focus(); 
    }

    savesSubscription() {
        // 1. Validate model
        if (!this.validate()) {
             return;
        }

        // 2. Create/add the new replication task
        const dto = this.editedSubscription().toDto();

        this.taskId = this.isAddingNewSubscriptionTask() ? 0 : this.taskId;

        //new saveSubscriptionTaskCommand(this.activeDatabase(), this.taskId, dto) // TODO...
        //    .execute()
        //    .done(() => this.goToOngoingTasksView());
    }
   
    cancelOperation() {
        this.goToOngoingTasksView();
    }

    private goToOngoingTasksView() {
        router.navigate(appUrl.forOngoingTasks(this.activeDatabase()));
    }

    private validate(): boolean {
        let valid = true;

        if (!this.isValid(this.editedSubscription().validationGroup))
            valid = false;

        return valid;
    }
}

export = editSubscriptionTask;