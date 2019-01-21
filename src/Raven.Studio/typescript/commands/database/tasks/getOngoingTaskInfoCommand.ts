import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand<T extends Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication |
                                          Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup |
                                          Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails> extends commandBase {

      private constructor(private db: database, private taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType, private taskId: number, private taskName?: string, private reportFailure: boolean = true) {
          super();
    }

    execute(): JQueryPromise<T> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to get info for ${this.taskType} task with id: ${this.taskId}. `, response.responseText, response.statusText);    
                }
            });
    }

    private getTaskInfo(): JQueryPromise<T> {
        const url = endpoints.databases.ongoingTasks.task;
        const args = this.taskName ? { key: this.taskId, type: this.taskType, taskName: this.taskName } :
            { key: this.taskId, type: this.taskType };

        return this.query<T>(url, args, this.db);
    }

    static forExternalReplication(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication>(db, "Replication", taskId);
    }
    
    static forPullReplicationSink(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink>(db, "PullReplicationAsSink", taskId);
    }

    static forPullReplicationHub(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.Replication.PullReplicationDefinitionAndCurrentConnections>(db, "PullReplicationAsHub", taskId);
    }

    static forSubscription(db: database, taskId: number, taskName: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails>(db, "Subscription", taskId, taskName);
    }

    static forBackup(db: database, taskId: number, reportFailure: boolean = true) { 
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>(db, "Backup", taskId, undefined, reportFailure);
    }

    static forRavenEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails>(db, "RavenEtl", taskId);
    }
    
    static forSqlEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails>(db, "SqlEtl", taskId);
    }
}

export = getOngoingTaskInfoCommand; 
