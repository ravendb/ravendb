import MockTasksService from "test/mocks/services/MockTasksService";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import assertUnreachable from "components/utils/assertUnreachable";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import ReplicationProcessProgress = Raven.Server.Documents.Replication.Stats.ReplicationProcessProgress;

export function commonInit(databaseType: "sharded" | "cluster" | "singleNode" = "sharded") {
    const { accessManager, license, databases } = mockStore;
    const { tasksService, licenseService } = mockServices;

    switch (databaseType) {
        case "sharded":
            databases.withActiveDatabase_Sharded();
            break;
        case "cluster":
            databases.withActiveDatabase_NonSharded_Cluster();
            break;
        case "singleNode":
            databases.withActiveDatabase_NonSharded_SingleNode();
            break;
        default:
            assertUnreachable(databaseType);
    }

    accessManager.with_securityClearance("ClusterAdmin");

    license.with_License();

    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");

    licenseService.withLimitsUsage();
    tasksService.withGetSubscriptionTaskInfo();
    tasksService.withGetSubscriptionConnectionDetails();
    tasksService.withGetExternalReplicationProgress((dto) => {
        dto.Results = [];
    });
    tasksService.withGetEtlProgress((dto) => {
        dto.Results = [];
    });
    tasksService.withGetInternalReplicationProgress((dto) => {
        dto.Results = [];
    });
}

export function markReplicationProcessCompleted(progress: ReplicationProcessProgress) {
    progress.Completed = true;
    progress.NumberOfAttachmentsToProcess = 0;
    progress.NumberOfCounterGroupsToProcess = 0;
    progress.NumberOfDocumentsToProcess = 0;
    progress.NumberOfDocumentTombstonesToProcess = 0;
    progress.NumberOfRevisionsToProcess = 0;
    progress.NumberOfTimeSeriesSegmentsToProcess = 0;
    progress.NumberOfTimeSeriesDeletedRangesToProcess = 0;
}

export function mockExternalReplicationProgress(tasksService: MockTasksService, completed: boolean) {
    if (completed) {
        tasksService.withGetExternalReplicationProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach(markReplicationProcessCompleted);
            });
        });
    } else {
        tasksService.withGetExternalReplicationProgress();
    }
}

export function mockEtlProgress(
    tasksService: MockTasksService,
    completed: boolean,
    disabled: boolean,
    emptyScript: boolean
) {
    if (completed) {
        tasksService.withGetEtlProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach((progress) => {
                    progress.Completed = true;
                    progress.Disabled = disabled;
                    progress.NumberOfDocumentsToProcess = 0;
                    progress.NumberOfTimeSeriesSegmentsToProcess = 0;
                    progress.NumberOfTimeSeriesDeletedRangesToProcess = 0;
                    progress.NumberOfCounterGroupsToProcess = 0;
                    progress.NumberOfDocumentTombstonesToProcess = 0;
                    if (emptyScript) {
                        progress.TotalNumberOfDocuments = 0;
                        progress.TotalNumberOfTimeSeriesDeletedRanges = 0;
                        progress.TotalNumberOfTimeSeriesSegments = 0;
                        progress.TotalNumberOfDocumentTombstones = 0;
                        progress.TotalNumberOfCounterGroups = 0;
                    }
                });
            });
        });
    } else {
        tasksService.withGetEtlProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach((progress) => {
                    progress.Disabled = disabled;
                });
            });
        });
    }
}
