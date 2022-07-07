import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;
import moment = require("moment");
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView;
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;

export class TasksStubs {
    static getTasksList(): OngoingTasksResult {
        return {
            OngoingTasksList: [
                TasksStubs.getRavenEtlListItem(),
                TasksStubs.getSqlListItem(),
                TasksStubs.getOlapListItem(),
                TasksStubs.getElasticSearchListItem(),
                TasksStubs.getPeriodicBackupListItem(),
                //TODO: kafka, rabbit, replication hub/sink, subscriptions
            ],
            PullReplications: [],
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
            ResponsibleNode: {
                NodeTag: "C",
                NodeUrl: "http://raven-c",
                ResponsibleNode: "C",
            },
            BackupDestinations: ["Local"],
            IsEncrypted: false,
            LastFullBackup: moment.utc().add(-7, "days").toISOString(),
            NextBackup: {
                IsFull: true,
                DateTime: moment.utc().add(2, "hours").toISOString(),
                TimeSpan: "02:00:00"
            },
            LastIncrementalBackup: moment.utc().add(-3, "days").toISOString(),
            LastExecutingNodeTag: "A",
            OnGoingBackup: null,
            RetentionPolicy: {
                Disabled: true,
                MinimumBackupAgeToKeep: "1.00:00:00"
            }
        }
    }

    static getRavenEtlListItem(): OngoingTaskRavenEtlListView {
        return {
            TaskName: "RavenETLTask",
            TaskId: 105,
            TaskType: "RavenEtl",
            ConnectionStringName: "RavenETL-CS",
            DestinationUrl: "http://target-etl:8080",
            ResponsibleNode: {
                NodeTag: "C",
                NodeUrl: "http://raven-c",
                ResponsibleNode: "C",
            },
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
            ResponsibleNode: {
                NodeTag: "C",
                NodeUrl: "http://raven-c",
                ResponsibleNode: "C",
            },
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
            ResponsibleNode: {
                NodeTag: "C",
                NodeUrl: "http://raven-c",
                ResponsibleNode: "C",
            },
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
        };
    }

    static getElasticSearchListItem(): OngoingTaskElasticSearchEtlListView {
        return {
            TaskName: "ElasticSearchTask",
            TaskId: 185,
            TaskType: "ElasticSearchEtl",
            ConnectionStringName: "ES-CS",
            ResponsibleNode: {
                NodeTag: "C",
                NodeUrl: "http://raven-c",
                ResponsibleNode: "C",
            },
            TaskState: "Enabled",
            Error: null,
            MentorNode: null,
            TaskConnectionStatus: "Active",
            NodesUrls: ["http://elastic1:8081", "http://elastic2:8081"],
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
