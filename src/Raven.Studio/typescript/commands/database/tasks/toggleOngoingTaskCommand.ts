import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class toggleOngoingTaskCommand extends commandBase {

    constructor(private db: database, private taskType: Raven.Client.ServerWide.Operations.OngoingTaskType, private taskId: number, private taskName: string, private disable: boolean) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.ServerWide.Operations.ModifyOngoingTaskResult> {
        const args = { key: this.taskId, type: this.taskType, disable: this.disable, taskName: this.taskName };
        const url = endpoints.databases.ongoingTasks.adminTasksState + this.urlEncodeArgs(args);
        const operationText = this.disable ? "disable" : "enable";
     
        return this.post(url, null, this.db)
            .done(() => this.reportSuccess(`Successfully ${operationText}d ${this.taskType} task`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to ${operationText} ${this.taskType} task. `, response.responseText));
    }
}

export = toggleOngoingTaskCommand; 
