import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand extends commandBase {

    constructor(private db: database, private taskType: Raven.Client.Server.Operations.OngoingTaskType, private taskId: number) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Server.Operations.GetTaskInfoResult> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for ${this.taskType} task with id: ${this.taskId}` + this.taskType, response.responseText, response.statusText);
            });
    }

    private getTaskInfo(): JQueryPromise<Raven.Client.Server.Operations.GetTaskInfoResult> {

        const url = endpoints.global.ongoingTasks.adminGetTask;
        const args = { name: this.db.name, key: this.taskId, type: this.taskType };

        return this.query<Raven.Client.Server.Operations.GetTaskInfoResult>(url, args);
    }
}

export = getOngoingTaskInfoCommand; 