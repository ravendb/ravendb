import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand<T extends Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication |
                                          Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails |
                                          Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails> extends commandBase {

    private readonly db: database;
    private readonly location: databaseLocationSpecifier;

    private readonly taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

    private readonly taskId: number;

    private readonly taskName?: string;

    private readonly reportFailure: boolean = true;

    public constructor(db: database, taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
                        location: databaseLocationSpecifier, taskId: number, taskName?: string, reportFailure = true) {
          super();
        this.reportFailure = reportFailure;
        this.taskName = taskName;
        this.taskId = taskId;
        this.taskType = taskType;
        this.db = db;
        this.location = location;
    }

    execute(): JQueryPromise<T> {
        return this.getTaskInfo()
            .fail((response: JQueryXHR) => {
                if (this.reportFailure) {
                    this.reportError(`Failed to get info for ${this.taskType} task with id: ${this.taskId}.`, response.responseText, response.statusText);
                }
            });
    }

    private getTaskInfo(): JQueryPromise<T> {
        const url = endpoints.databases.ongoingTasks.task;
       
        const args = {
            ...this.getArgsToUse(),
            ...this.location
        };

        return this.query<T>(url, args, this.db);
    }

    static forExternalReplication(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication>(db, "Replication", null, taskId);
    }
    
    static forPullReplicationSink(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink>(db, "PullReplicationAsSink", null, taskId);
    }

    static forSubscription(db: database, location: databaseLocationSpecifier, taskId: number, taskName: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Subscriptions.SubscriptionStateWithNodeDetails>(db, "Subscription", location, taskId, taskName);
    }

    static forBackup(db: database, taskId: number, reportFailure = true) { 
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>(db, "Backup", null, taskId, undefined, reportFailure);
    }

    static forRavenEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails>(db, "RavenEtl", null, taskId);
    }
    
    static forSqlEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails>(db, "SqlEtl", null, taskId);
    }

    static forOlapEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlDetails>(db, "OlapEtl", null, taskId);
    }

    static forElasticSearchEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlDetails>(db, "ElasticSearchEtl", null, taskId);
    }

    private getArgsToUse() {
        if (this.taskName) {
            return {
                key: this.taskId,
                type: this.taskType,
                taskName: this.taskName
            }
        }

        return {
            key: this.taskId,
            type: this.taskType
        }
    }

    static forQueueEtl(db: database, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlDetails>(db, "QueueEtl", null, taskId);
    }
}

export = getOngoingTaskInfoCommand; 
