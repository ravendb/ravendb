import { AutoMockService, MockedValue } from "./AutoMockService";
import TasksService from "components/services/TasksService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { TasksStubs } from "test/stubs/TasksStubs";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import GetPeriodicBackupStatusOperationResult = Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult;
import collectionsStats = require("models/database/documents/collectionsStats");
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { SharedStubs } from "test/stubs/SharedStubs";

export default class MockTasksService extends AutoMockService<TasksService> {
    constructor() {
        super(new TasksService());
    }

    withGetTasks(dto?: MockedValue<OngoingTasksResult>) {
        return this.mockResolvedValue(this.mocks.getOngoingTasks, dto, TasksStubs.getTasksList());
    }

    withGetEtlProgress(dto?: MockedValue<resultsDto<EtlTaskProgress>>) {
        return this.mockResolvedValue(this.mocks.getEtlProgress, dto, TasksStubs.getEtlTasksProgress());
    }

    withGetManualBackup(dto?: MockedValue<GetPeriodicBackupStatusOperationResult>) {
        return this.mockResolvedValue(this.mocks.getManualBackup, dto, TasksStubs.getManualBackup());
    }

    withGetSubscriptionTaskInfo(
        dto?: MockedValue<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription>
    ) {
        return this.mockResolvedValue(this.mocks.getSubscriptionTaskInfo, dto, TasksStubs.getSubscription());
    }

    withGetSubscriptionConnectionDetails(
        dto?: MockedValue<Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails>
    ) {
        return this.mockResolvedValue(
            this.mocks.getSubscriptionConnectionDetails,
            dto,
            TasksStubs.subscriptionConnectionDetails()
        );
    }

    withGetSampleDataClasses(dto?: MockedValue<string>) {
        return this.mockResolvedValue(this.mocks.getSampleDataClasses, dto, TasksStubs.getSampleDataClasses());
    }

    withFetchCollectionsStats(dto?: MockedValue<Partial<collectionsStats>>) {
        return this.mockResolvedValue(this.mocks.fetchCollectionsStats, dto, TasksStubs.emptyCollectionsStats());
    }

    withConnectionStrings(dto?: Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult) {
        return this.mockResolvedValue(this.mocks.getConnectionStrings, dto, DatabasesStubs.connectionStrings());
    }

    withTestClusterNodeConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testClusterNodeConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestSqlConnectionString(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testSqlConnectionString,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestKafkaServerConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testKafkaServerConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestRabbitMqServerConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testRabbitMqServerConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestAzureQueueStorageServerConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testAzureQueueStorageServerConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestElasticSearchNodeConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testElasticSearchNodeConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withBackupLocation(dto?: Raven.Server.Web.Studio.DataDirectoryResult) {
        return this.mockResolvedValue(this.mocks.getBackupLocation, dto, TasksStubs.backupLocation());
    }

    withLocalFolderPathOptions(dto?: Raven.Server.Web.Studio.FolderPathOptions) {
        return this.mockResolvedValue(this.mocks.getLocalFolderPathOptions, dto, TasksStubs.localFolderPathOptions());
    }
}
