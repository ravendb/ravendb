import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteOngoingTaskCommand extends commandBase {
    
    constructor(private db: database, private taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private taskId: number, private taskName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const args = { id: this.taskId, type: this.taskType, taskName: this.taskName };

        // Subscription is the only task that needs only *User* authentication for delete task
        // All others use *Admin* authentication
        const url = this.taskType === "Subscription" ? endpoints.databases.ongoingTasks.subscriptionTasks :
                                                       endpoints.databases.ongoingTasks.adminTasks;

        return this.del<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>(url + this.urlEncodeArgs(args), null, this.db)
            .done(() => this.reportSuccess(`Successfully deleted ${this.taskType} task`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete ${this.taskType}`, response.responseText));
    }
}

export = deleteOngoingTaskCommand;
