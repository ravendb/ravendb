import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import moment = require("moment");
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView;
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import PullReplicationDefinition = Raven.Client.Documents.Operations.Replication.PullReplicationDefinition;
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;

export class TasksStubs {
    static getTasksList(): OngoingTasksResult {
        const emptyPullReplicationDefinition = TasksStubs.getReplicationHubDefinition();
        emptyPullReplicationDefinition.TaskId++;
        emptyPullReplicationDefinition.Name = "EmptyHub";

        return {
            OngoingTasksList: [
                TasksStubs.getRavenEtlListItem(),
                TasksStubs.getSqlListItem(),
                TasksStubs.getOlapListItem(),
                TasksStubs.getElasticSearchListItem(),
                TasksStubs.getPeriodicBackupListItem(),
                TasksStubs.getKafkaListItem(),
                TasksStubs.getRabbitListItem(),
                TasksStubs.getReplicationSinkListItem(),
                TasksStubs.getReplicationHubListItem(),
                TasksStubs.getExternalReplicationListItem(),
                TasksStubs.getSubscriptionListItem(),
            ],
            PullReplications: [TasksStubs.getReplicationHubDefinition(), emptyPullReplicationDefinition],
            SubscriptionsCount: 0,
        };
    }

    static getTasksProgress(): resultsDto<EtlTaskProgress> {
        return {
            Results: [
                TasksStubs.getRavenEtlProgress(),
                TasksStubs.getSqlProgress(),
                TasksStubs.getOlapProgress(),
                TasksStubs.getElasticsearchProgress(),
                TasksStubs.getKafkaProgress(),
                TasksStubs.getRabbitProgress(),
            ],
        };
    }

    static getRavenEtlProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getRavenEtlListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Raven");
    }

    static getSqlProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getSqlListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Sql");
    }

    static getOlapProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getOlapListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Olap");
    }

    static getKafkaProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getKafkaListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Queue");
    }

    static getRabbitProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getRabbitListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Queue");
    }

    static getElasticsearchProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getElasticSearchListItem().TaskName;
        return TasksStubs.getEtlProgress(taskName, "ElasticSearch");
    }

    static getPeriodicBackupListItem(): OngoingTaskBackup {
        return {
            TaskName: "Raven Backup",
            TaskId: 192,
            TaskType: "Backup",
            TaskConnectionStatus: "Active",
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            BackupType: "Backup",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            BackupDestinations: ["Local"],
            IsEncrypted: false,
            LastFullBackup: moment.utc().add(-7, "days").toISOString(),
            NextBackup: {
                IsFull: true,
                DateTime: moment.utc().add(2, "hours").toISOString(),
                TimeSpan: "02:00:00",
            },
            LastIncrementalBackup: moment.utc().add(-3, "days").toISOString(),
            LastExecutingNodeTag: "A",
            OnGoingBackup: null,
            RetentionPolicy: {
                Disabled: true,
                MinimumBackupAgeToKeep: "1.00:00:00",
            },
        };
    }

    static getExternalReplicationListItem(): OngoingTaskReplication {
        return {
            TaskName: "ExternalReplicationTask",
            TaskId: 438,
            TaskType: "Replication",
            MentorNode: null,
            Error: null,
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            TaskConnectionStatus: "Active",
            ConnectionStringName: "ExtRep-CS",
            DestinationUrl: "http://target-raven:8080",
            DelayReplicationFor: null,
            TopologyDiscoveryUrls: ["http://target-raven:8080"],
            DestinationDatabase: "r-ext",
        };
    }

    static getRavenEtlListItem(): OngoingTaskRavenEtlListView {
        return {
            TaskName: "RavenETLTask",
            TaskId: 105,
            TaskType: "RavenEtl",
            ConnectionStringName: "RavenETL-CS",
            DestinationUrl: "http://target-etl:8080",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            DestinationDatabase: "target-etl-db",
            MentorNode: null,
            TaskConnectionStatus: "Active",
            TopologyDiscoveryUrls: ["http://url1", "http://url2"],
        };
    }

    static getSqlListItem(): OngoingTaskSqlEtlListView {
        return {
            TaskName: "SqlTask",
            TaskId: 115,
            TaskType: "SqlEtl",
            ConnectionStringName: "SQL-CS",
            DestinationDatabase: "sql-db1",
            DestinationServer: "mssql:1521",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
            ConnectionStringDefined: true,
        };
    }

    static getOlapListItem(): OngoingTaskOlapEtlListView {
        return {
            TaskName: "OlapTask",
            TaskId: 145,
            TaskType: "OlapEtl",
            ConnectionStringName: "OLAP-CS",
            Destination: "TargetOLAP",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
        };
    }

    static getKafkaListItem(): OngoingTaskQueueEtlListView {
        return {
            TaskName: "KafkaTask",
            TaskId: 302,
            TaskType: "QueueEtl",
            ConnectionStringName: "Kafka-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            TaskConnectionStatus: "Active",
            MentorNode: null,
            BrokerType: "Kafka",
            Url: "localhost:9092",
        };
    }

    static getRabbitListItem(): OngoingTaskQueueEtlListView {
        return {
            TaskName: "RabbitTask",
            TaskId: 303,
            TaskType: "QueueEtl",
            ConnectionStringName: "Rabbit-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            TaskConnectionStatus: "Active",
            MentorNode: null,
            Url: "localhost:6006",
            BrokerType: "RabbitMq",
        };
    }

    static getReplicationSinkListItem(): OngoingTaskPullReplicationAsSink {
        return {
            TaskName: "ReplicationSinkTask",
            TaskId: 243,
            TaskType: "PullReplicationAsSink",
            MentorNode: null,
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            HubDefinitionName: "HubName",
            TaskConnectionStatus: "Active",
            ConnectionStringName: "Hub-cs",
            Mode: "SinkToHub",
            HubName: "HubName",
            DestinationDatabase: "hub-db",
            DestinationUrl: "http://hub-server:8080",
            AllowedHubToSinkPaths: null,
            AllowedSinkToHubPaths: null,
            TopologyDiscoveryUrls: ["http://hub-server:8080"],
            AccessName: null,
            CertificatePublicKey: null,
        };
    }

    static getReplicationHubListItem(): OngoingTaskPullReplicationAsHub {
        return {
            TaskName: "sink1",
            TaskId: 287,
            TaskConnectionStatus: "Active",
            TaskState: "Enabled",
            Error: null,
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskType: "PullReplicationAsHub",
            MentorNode: null,
            DestinationDatabase: "target-hub-db",
            DestinationUrl: "http://target-hub-host:8080",
            DelayReplicationFor: null,
        };
    }

    static getReplicationHubDefinition(): PullReplicationDefinition {
        return {
            TaskId: 287,
            MentorNode: null,
            Disabled: false,
            Mode: "SinkToHub",
            DelayReplicationFor: null,
            Certificates: null,
            PreventDeletionsMode: "None",
            Name: "hub1",
            WithFiltering: false,
        };
    }

    static getElasticSearchListItem(): OngoingTaskElasticSearchEtlListView {
        return {
            TaskName: "ElasticSearchTask",
            TaskId: 185,
            TaskType: "ElasticSearchEtl",
            ConnectionStringName: "ES-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
            NodesUrls: ["http://elastic1:8081", "http://elastic2:8081"],
        };
    }

    static getSubscriptionListItem(): OngoingTaskSubscription {
        return {
            TaskName: "NewOrdersSubTask",
            TaskId: 524,
            TaskState: "Enabled",
            Error: null,
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskConnectionStatus: "Active",
            MentorNode: null,
            TaskType: "Subscription",
            Disabled: false,
            SubscriptionId: 101,
            ChangeVectorForNextBatchStartingPoint: "A:5,B:3",
            LastBatchAckTime: moment.utc().add(-1, "hours").toISOString(),
            Query: "from Orders",
            SubscriptionName: "NamedSubscription",
            LastClientConnectionTime: moment.utc().add(-2, "hours").toISOString(),
            ChangeVectorForNextBatchStartingPointPerShard: null,
        };
    }

    private static getResponsibleNode(): Raven.Client.ServerWide.Operations.NodeId {
        return {
            NodeTag: "C",
            NodeUrl: "http://raven-c",
            ResponsibleNode: "C",
        };
    }

    private static getEtlProgress(taskName: string, etlType: EtlType): EtlTaskProgress {
        return {
            TaskName: taskName,
            ProcessesProgress: [
                {
                    AverageProcessedPerSecond: 36.7,
                    Disabled: false,
                    Completed: false,
                    NumberOfDocumentsToProcess: 524,
                    TotalNumberOfDocuments: 1024,
                    NumberOfCounterGroupsToProcess: 108,
                    TotalNumberOfCounterGroups: 200,
                    NumberOfDocumentTombstonesToProcess: 123,
                    TotalNumberOfDocumentTombstones: 223,
                    NumberOfTimeSeriesDeletedRangesToProcess: 0,
                    TotalNumberOfTimeSeriesDeletedRanges: 0,
                    TotalNumberOfTimeSeriesSegments: 0,
                    NumberOfTimeSeriesSegmentsToProcess: 0,
                    TransformationName: "Script #1",
                },
            ],
            EtlType: etlType,
        };
    }
}
