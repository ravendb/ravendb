import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteOngoingTaskCommand extends commandBase {
    
    private db: database;

    private taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

    private taskId: number;

    private taskName: string;

    constructor(db: database, taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, taskId: number, taskName: string) {
        super();
        this.taskName = taskName;
        this.taskId = taskId;
        this.taskType = taskType;
        this.db = db;
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult> {
        const args = { id: this.taskId, type: this.taskType, taskName: this.taskName };

        // Subscription is the only task that needs only *User* authentication for delete task
        // All others use *Admin* authentication
        const url = this.taskType === "Subscription" ? endpoints.databases.ongoingTasks.subscriptionTasks :
                                                       endpoints.databases.ongoingTasks.adminTasks;

        return this.del<Raven.Client.Documents.Operations.OngoingTasks.ModifyOngoingTaskResult>(url + this.urlEncodeArgs(args), null, this.db)
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete ${this.taskType}`, response.responseText));
    }
}

export = deleteOngoingTaskCommand;
