import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class toggleOngoingTaskCommand extends commandBase {

    constructor(private db: database, private taskType: Raven.Client.Server.Operations.OngoingTaskType, private taskId: number, private disable: boolean) {
        super();
    }

    execute(): JQueryPromise<any> {
        const args = { name: this.db.name, key: this.taskId, type: this.taskType, disable: this.disable };
        const url = endpoints.global.ongoingTasks.adminTasksStatus + this.urlEncodeArgs(args);
        const operationText = this.disable ? "disable" : "enable";
     
        return this.patch(url, null)
            .done(() => this.reportSuccess(`Successfully ${operationText}d ${this.taskType} task`))
            .fail((response: JQueryXHR) => this.reportError(`Failed to ${operationText} ${this.taskType} task. `, response.responseText));
    }
}

export = toggleOngoingTaskCommand; 
