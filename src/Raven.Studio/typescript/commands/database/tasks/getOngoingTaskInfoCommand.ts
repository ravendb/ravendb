import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand<T extends Raven.Client.ServerWide.Operations.OngoingTaskReplication |
                                          Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails |
                                          Raven.Client.ServerWide.Operations.OngoingTaskBackup |
                                          Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl> extends commandBase {

      private constructor(private db: database, private taskType: Raven.Client.ServerWide.Operations.OngoingTaskType, private taskId: number, private taskName?: string) {
          super();
    }

    execute(): JQueryPromise<T> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to get info for ${this.taskType} task with id: ${this.taskId}. `, response.responseText, response.statusText);
            });
    }

    private getTaskInfo(): JQueryPromise<T> {
        const url = endpoints.databases.ongoingTasks.task;
        const args = this.taskName ? { key: this.taskId, type: this.taskType, taskName: this.taskName } :
            { key: this.taskId, type: this.taskType };

        return this.query<T>(url, args, this.db);
    }

    static forExternalReplication(db: database, taskId: number, taskName?: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.ServerWide.Operations.OngoingTaskReplication>(db, "Replication", taskId, taskName);
    }

    static forSubscription(db: database, taskId: number, taskName?: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails>(db, "Subscription", taskId, taskName);
    }

    static forBackup(db: database, taskId: number, taskName?: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.ServerWide.Operations.OngoingTaskBackup>(db, "Backup", taskId, taskName);
    }

    static forRavenEtl(db: database, taskId: number, taskName?: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.ServerWide.Operations.OngoingTaskRavenEtl>(db, "RavenEtl", taskId, taskName);
    }
}

export = getOngoingTaskInfoCommand; 
