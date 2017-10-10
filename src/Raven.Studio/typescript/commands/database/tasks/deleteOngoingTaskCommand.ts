import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteOngoingTaskCommand extends commandBase {
    
    constructor(private db: database, private taskType: Raven.Client.ServerWide.Operations.OngoingTaskType, private taskId: number, private taskName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        const args = { id: this.taskId, type: this.taskType, taskName: this.taskName };

        // Subscription is the only task that needs only *User* authenication for delete task
        // All others use *Admin* authentication
        const url = this.taskType === "Subscription" ? endpoints.databases.ongoingTasks.subscriptionTasks :
                                                       endpoints.databases.ongoingTasks.adminTasks;

        return this.del<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult>(url + this.urlEncodeArgs(args), null, this.db)
            .done(() => this.reportSuccess(`Successfullly deleted ${this.taskType} task`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete ${this.taskType}`, response.responseText));
    }
}

export = deleteOngoingTaskCommand;
