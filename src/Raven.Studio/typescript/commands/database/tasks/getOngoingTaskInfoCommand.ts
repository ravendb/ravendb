import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class getOngoingTaskInfoCommand<T extends Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl |
    Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink> extends commandBase {

    private readonly db: database | string;
    private readonly nodeTag: string;

    private readonly taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;

    private readonly taskId: number;

    private readonly taskName?: string;

    private readonly reportFailure: boolean = true;

    public constructor(db: database | string, taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
                        nodeTag: string | undefined, taskId: number, taskName?: string, reportFailure = true) {
          super();
        this.reportFailure = reportFailure;
        this.taskName = taskName;
        this.taskId = taskId;
        this.taskType = taskType;
        this.db = db;
        this.nodeTag = nodeTag;
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
            nodeTag: this.nodeTag
        };

        return this.query<T>(url, args, this.db);
    }

    static forExternalReplication(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication>(db, "Replication", null, taskId);
    }
    
    static forPullReplicationSink(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink>(db, "PullReplicationAsSink", null, taskId);
    }

    static forSubscription(db: database | string, taskId: number, taskName: string, nodeTag?: string) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription>(db, "Subscription", nodeTag, taskId, taskName);
    }

    static forBackup(db: database | string, taskId: number, reportFailure = true) { 
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup>(db, "Backup", null, taskId, undefined, reportFailure);
    }

    static forRavenEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl>(db, "RavenEtl", null, taskId);
    }
    
    static forSqlEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl>(db, "SqlEtl", null, taskId);
    }

    static forSnowflakeEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl>(db, "SnowflakeEtl", null, taskId);
    }

    static forOlapEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl>(db, "OlapEtl", null, taskId);
    }

    static forElasticSearchEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl>(db, "ElasticSearchEtl", null, taskId);
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

    static forQueueEtl(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl>(db, "QueueEtl", null, taskId);
    }

    static forQueueSink(db: database | string, taskId: number) {
        return new getOngoingTaskInfoCommand<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink>(db, "QueueSink", null, taskId);
    }
}

export = getOngoingTaskInfoCommand; 
