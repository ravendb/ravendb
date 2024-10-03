import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import moment = require("moment");
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import PullReplicationDefinition = Raven.Client.Documents.Operations.Replication.PullReplicationDefinition;
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
import GetPeriodicBackupStatusOperationResult = Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult;
import CloudUploadStatus = Raven.Client.Documents.Operations.Backups.CloudUploadStatus;
import OngoingTaskRavenEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import OngoingTaskSqlEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl;
import OngoingTaskOlapEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl;
import OngoingTaskQueueEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl;
import OngoingTaskElasticSearchEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl;
import collectionsStats = require("models/database/documents/collectionsStats");
import collection = require("models/database/documents/collection");
import OngoingTaskQueueSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink;
import OngoingTaskSnowflakeEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl;

export class TasksStubs {
    static getTasksList(): OngoingTasksResult {
        const emptyPullReplicationDefinition = TasksStubs.getReplicationHubDefinition();
        emptyPullReplicationDefinition.TaskId++;
        emptyPullReplicationDefinition.Name = "EmptyHub";

        return {
            OngoingTasks: [
                TasksStubs.getRavenEtl(),
                TasksStubs.getSql(),
                TasksStubs.getSnowflake(),
                TasksStubs.getOlap(),
                TasksStubs.getElasticSearch(),
                TasksStubs.getPeriodicBackupListItem(),
                TasksStubs.getKafkaEtl(),
                TasksStubs.getRabbitEtl(),
                TasksStubs.getAzureQueueStorageEtl(),
                TasksStubs.getKafkaSink(),
                TasksStubs.getRabbitSink(),
                TasksStubs.getReplicationSink(),
                TasksStubs.getReplicationHub(),
                TasksStubs.getExternalReplicationListItem(),
                TasksStubs.getSubscription(),
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
                TasksStubs.getSnowflakeProgress(),
                TasksStubs.getOlapProgress(),
                TasksStubs.getElasticsearchProgress(),
                TasksStubs.getKafkaProgress(),
                TasksStubs.getRabbitProgress(),
                TasksStubs.getAzureQueueStorageProgress(),
            ],
        };
    }

    static subscriptionConnectionDetails(): Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails {
        return {
            Results: [
                {
                    WorkerId: "worker-1",
                    ClientUri: "http://127.0.0.1:5344",
                    Strategy: "OpenIfFree",
                },
            ],
            SubscriptionMode: "None",
        };
    }

    static getManualBackup(): GetPeriodicBackupStatusOperationResult {
        const emptyUpload: CloudUploadStatus = {
            LastFullBackup: null as string,
            LastIncrementalBackup: null as string,
            FullBackupDurationInMs: null as number,
            IncrementalBackupDurationInMs: null as number,
            Exception: null as string,
            Skipped: true,
            UploadProgress: {
                UploadType: "Regular",
                UploadState: "PendingUpload",
                UploadedInBytes: 0,
                TotalInBytes: 0,
                BytesPutsPerSec: 0.0,
                UploadTimeInMs: 0,
            },
        };

        return {
            Status: {
                TaskId: 0,
                BackupType: "Backup",
                IsFull: true,
                NodeTag: "A",
                LastFullBackup: "2022-08-04T12:25:12.9402638Z",
                LastIncrementalBackup: null,
                LastFullBackupInternal: "2022-08-04T12:25:12.9402638Z",
                LastIncrementalBackupInternal: null,
                LocalBackup: {
                    LastFullBackup: "2022-08-04T12:25:52.3441072Z",
                    LastIncrementalBackup: null,
                    FullBackupDurationInMs: 2429,
                    IncrementalBackupDurationInMs: null,
                    Exception: null,
                    BackupDirectory: "c:\\temp\\backup22\\2022-08-04-14-25-12.ravendb-db1-A-backup",
                    FileName: null,
                    TempFolderUsed: false,
                },
                UploadToS3: emptyUpload,
                UploadToGlacier: emptyUpload,
                UploadToAzure: emptyUpload,
                UploadToGoogleCloud: emptyUpload,
                UploadToFtp: emptyUpload,
                LastEtag: 8806,
                LastRaftIndex: { LastEtag: 8 },
                FolderName: "2022-08-04-14-25-12.ravendb-db1-A-backup",
                DurationInMs: 2442,
                LocalRetentionDurationInMs: 0,
                Version: 0,
                Error: null,
                LastOperationId: 3,
                LastDatabaseChangeVector:
                    "A:8806-9igKNP9Qh0WWnuROUXOVjQ, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ",
                IsEncrypted: false,
            },
        };
    }

    static getRavenEtlProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getRavenEtl().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Raven");
    }

    static getSqlProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getSql().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Sql");
    }

    static getSnowflakeProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getSnowflake().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Snowflake");
    }

    static getOlapProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getOlap().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Olap");
    }

    static getKafkaProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getKafkaEtl().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Queue", "bVhBBojWnEOKrsszfuQ+Yg-tst-kafka_Script #1");
    }

    static getRabbitProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getRabbitEtl().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Queue");
    }

    static getAzureQueueStorageProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getAzureQueueStorageEtl().TaskName;
        return TasksStubs.getEtlProgress(taskName, "Queue");
    }

    static getElasticsearchProgress(): EtlTaskProgress {
        const taskName = TasksStubs.getElasticSearch().TaskName;
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
            PinToMentorNode: false,
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
            PinToMentorNode: false,
        };
    }

    static getRavenEtl(): OngoingTaskRavenEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getSql(): OngoingTaskSqlEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getSnowflake(): OngoingTaskSnowflakeEtl {
        return {
            TaskName: "SnowflakeTask",
            TaskId: 116,
            TaskType: "SnowflakeEtl",
            ConnectionStringName: "Snowflake-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
            ConnectionString: "SNOWFLAKE-CS",
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getOlap(): OngoingTaskOlapEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getKafkaEtl(): OngoingTaskQueueEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getRabbitEtl(): OngoingTaskQueueEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getAzureQueueStorageEtl(): OngoingTaskQueueEtl {
        return {
            TaskName: "AzureQueueStorageTask",
            TaskId: 304,
            TaskType: "QueueEtl",
            ConnectionStringName: "AQS-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            TaskConnectionStatus: "Active",
            MentorNode: null,
            Url: "localhost:6056",
            BrokerType: "AzureQueueStorage",
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getKafkaSink(): OngoingTaskQueueSink {
        return {
            TaskName: "KafkaSinkTask",
            TaskId: 705,
            TaskType: "QueueSink",
            ConnectionStringName: "Kafka-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            TaskConnectionStatus: "Active",
            MentorNode: null,
            BrokerType: "Kafka",
            Url: "localhost:9092",
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getRabbitSink(): OngoingTaskQueueEtl {
        return {
            TaskName: "RabbitSinkTask",
            TaskId: 706,
            TaskType: "QueueSink",
            ConnectionStringName: "Rabbit-CS",
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
            TaskConnectionStatus: "Active",
            MentorNode: null,
            Url: "localhost:6006",
            BrokerType: "RabbitMq",
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getReplicationSink(): OngoingTaskPullReplicationAsSink {
        return {
            TaskName: "ReplicationSinkTask",
            TaskId: 243,
            TaskType: "PullReplicationAsSink",
            MentorNode: null,
            ResponsibleNode: TasksStubs.getResponsibleNode(),
            TaskState: "Enabled",
            Error: null,
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
            PinToMentorNode: false,
        };
    }

    static getReplicationHub(): OngoingTaskPullReplicationAsHub {
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
            PinToMentorNode: false,
        };
    }

    static getReplicationHubDefinition(): PullReplicationDefinition {
        return {
            TaskId: 287,
            MentorNode: null,
            Disabled: false,
            Mode: "SinkToHub",
            DelayReplicationFor: null,
            PreventDeletionsMode: "None",
            Name: "hub1",
            WithFiltering: false,
            PinToMentorNode: false,
        };
    }

    static getElasticSearch(): OngoingTaskElasticSearchEtl {
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
            PinToMentorNode: false,
            Configuration: null,
        };
    }

    static getSubscription(): OngoingTaskSubscription {
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
            ArchivedDataProcessingBehavior: null,
            SubscriptionId: 524,
            ChangeVectorForNextBatchStartingPoint: "B:884-7YtyJhmi/k+as1eW7RRJWQ, A:856-TtyicrkQAUKtvYiwGx0yoA",
            LastBatchAckTime: moment.utc().add(-1, "hours").toISOString(),
            Query: "from Orders",
            SubscriptionName: "NamedSubscription",
            LastClientConnectionTime: moment.utc().add(-2, "hours").toISOString(),
            ChangeVectorForNextBatchStartingPointPerShard: null,
            PinToMentorNode: false,
        };
    }

    private static getResponsibleNode(): Raven.Client.ServerWide.Operations.NodeId {
        return {
            NodeTag: "C",
            NodeUrl: "http://raven-c",
            ResponsibleNode: "C",
        };
    }

    private static getEtlProgress(taskName: string, etlType: EtlType, transactionalId?: string): EtlTaskProgress {
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
                    TransactionalId: transactionalId,
                },
            ],
            EtlType: etlType,
        };
    }

    static getSampleDataClasses(): string {
        return `using System;
using System.Collections.Generic;
using Raven.Client.Documents.Session.TimeSeries;

namespace Orders
{
    public sealed class Company
    {
        public string Id { get; set; }
        public string ExternalId { get; set; }
        public string Name { get; set; }
        public Contact Contact { get; set; }
        public Address Address { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }

        public sealed class StockPrice
        {
            [TimeSeriesValue(0)] public double Open { get; set; }
            [TimeSeriesValue(1)] public double Close { get; set; }
            [TimeSeriesValue(2)] public double High { get; set; }
            [TimeSeriesValue(3)] public double Low { get; set; }
            [TimeSeriesValue(4)] public double Volume { get; set; }
        }
    }
    // ...
`;
    }

    static emptyCollectionsStats(): Partial<collectionsStats> {
        return {
            collections: [],
        };
    }

    static notEmptyCollectionsStats(): Partial<collectionsStats> {
        return {
            collections: [new collection("some-collection-name", 2)],
        };
    }

    static backupLocation(): Raven.Server.Web.Studio.DataDirectoryResult {
        return {
            List: [
                {
                    NodeTag: "A",
                    FullPath: "/",
                    FreeSpaceInBytes: 6126075904,
                    FreeSpaceHumane: "5.705 GBytes",
                    TotalSpaceInBytes: 20738408448,
                    TotalSpaceHumane: "19.314 GBytes",
                    Error: "Cannot write to directory path: /",
                },
            ],
        };
    }

    static localFolderPathOptions(): Raven.Server.Web.Studio.FolderPathOptions {
        return {
            List: ["/bin", "/boot", "/data", "/dev", "/etc"],
        };
    }
}
