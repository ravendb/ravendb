import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand extends commandBase {

    constructor(private db: database, private taskType: Raven.Client.Server.Operations.OngoingTaskType, private taskId: number, private taskName?: string) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Server.Operations.GetTaskInfoResult | Raven.Client.Documents.Subscriptions.SubscriptionState> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for ${this.taskType} task with id: ${this.taskId}. `, response.responseText, response.statusText);
            });
    }

    private getTaskInfo(): JQueryPromise<Raven.Client.Server.Operations.GetTaskInfoResult | Raven.Client.Documents.Subscriptions.SubscriptionState> {

        const url = endpoints.global.ongoingTasks.adminGetTask;
     
        const args = this.taskName ? { name: this.db.name, key: this.taskId, type: this.taskType, taskName: this.taskName } :
                                     { name: this.db.name, key: this.taskId, type: this.taskType };
     
        return this.query<any>(url, args);
    }
}

export = getOngoingTaskInfoCommand; 