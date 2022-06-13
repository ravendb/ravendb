import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;

export class TasksStubs {
    static getTasksList(): OngoingTasksResult {
        return {
            OngoingTasksList: [TasksStubs.getRavenEtlListItem()],
            PullReplications: [],
            SubscriptionsCount: 0,
        };
    }

    static getTasksProgress(): resultsDto<EtlTaskProgress> {
        return {
            Results: [TasksStubs.getRavenEtlProgress()],
        };
    }

    static getRavenEtlProgress(): EtlTaskProgress {
        return {
            TaskName: "RavenETLTask",
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
            EtlType: "Raven",
        };
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
}
