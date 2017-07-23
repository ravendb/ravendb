import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteOngoingTaskCommand extends commandBase {
    
    constructor(private db: database, private taskType: Raven.Client.Server.Operations.OngoingTaskType, private taskId: number, private taskName: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.ModifyOngoingTaskResult> {
        const args = { name: this.db.name, id: this.taskId, type: this.taskType, taskName: this.taskName };
        const url = endpoints.global.ongoingTasks.adminTasks + this.urlEncodeArgs(args);

        return this.del<Raven.Client.Server.Operations.ModifyOngoingTaskResult>(url, null)
            .done(() => this.reportSuccess(`Successfullly deleted ${this.taskType} task`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to delete ${this.taskType}`, response.responseText));
    }
}

export = deleteOngoingTaskCommand; 