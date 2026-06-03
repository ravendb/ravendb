import { AutoMockService, MockedValue } from "./AutoMockService";
import TasksService from "components/services/TasksService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { TasksStubs } from "test/stubs/TasksStubs";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import GetPeriodicBackupStatusOperationResult = Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult;
import collectionsStats = require("models/database/documents/collectionsStats");
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import { SharedStubs } from "test/stubs/SharedStubs";
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;
import { mockJQueryError } from "test/mocks/utils";
import EtlErrors = Raven.Server.Documents.ETL.Stats.TaskErrors;
import EtlTaskStats = Raven.Server.Documents.ETL.Stats.EtlTaskStats;

export default class MockTasksService extends AutoMockService<TasksService> {
    constructor() {
        super(new TasksService());
    }

    withGetTasks(dto?: MockedValue<OngoingTasksResult>) {
        return this.mockResolvedValue(this.mocks.getOngoingTasks, dto, TasksStubs.getTasksList());
    }

    withThrowingGetTasks(
        shouldThrow: (databaseName: string, location: databaseLocationSpecifier) => boolean,
        dto?: MockedValue<OngoingTasksResult>
    ) {
        const mockedValue = this.createValue(dto, TasksStubs.getTasksList());
        return this.mocks.getOngoingTasks.mockImplementation(async (db, location) => {
            if (shouldThrow(db, location)) {
                throw mockJQueryError("This is error message");
            } else {
                return mockedValue;
            }
        });
    }

    withGetEtlProgress(dto?: MockedValue<resultsDto<EtlTaskProgress>>) {
        return this.mockResolvedValue(this.mocks.getEtlProgress, dto, TasksStubs.getEtlTasksProgress());
    }

    withGetExternalReplicationProgress(dto?: MockedValue<resultsDto<ReplicationTaskProgress>>) {
        return this.mockResolvedValue(
            this.mocks.getReplicationProgress,
            dto,
            TasksStubs.getExternalReplicationTasksProgress()
        );
    }

    withGetInternalReplicationProgress(dto?: MockedValue<resultsDto<InternalReplicationTaskProgress>>) {
        return this.mockResolvedValue(
            this.mocks.getInternalReplicationProgress,
            dto,
            TasksStubs.getInternalReplicationTasksProgress()
        );
    }

    withGetManualBackup(dto?: MockedValue<GetPeriodicBackupStatusOperationResult>) {
        return this.mockResolvedValue(this.mocks.getManualBackup, dto, TasksStubs.getManualBackup());
    }

    withGetSubscriptionTaskInfo(
        dto?: MockedValue<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription>
    ) {
        return this.mockResolvedValue(this.mocks.getSubscriptionTaskInfo, dto, TasksStubs.getSubscription());
    }

    withGetCdcSinkTaskInfo(dto?: MockedValue<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink>) {
        return this.mockResolvedValue(this.mocks.getCdcSinkTaskInfo, dto, TasksStubs.getCdcSink());
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

    withTestCdcSink(dto?: Raven.Client.Documents.Operations.CdcSink.Test.TestCdcSinkMappingResult) {
        return this.mockResolvedValue(this.mocks.testCdcSink, dto, TasksStubs.testCdcSink());
    }

    withGetCdcSinkTaskSchema(dto?: MockedValue<Raven.Client.Documents.Operations.CdcSink.Schema.CdcSinkSourceSchema>) {
        return this.mockResolvedValue(this.mocks.getCdcSinkTaskSchema, dto, TasksStubs.cdcSinkTaskSchema());
    }

    withTestSnowflakeConnectionString(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
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

    withTestAmazonSqsServerConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testAmazonSqsServerConnection,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestAzureServiceBusServerConnection(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testAzureServiceBusServerConnection,
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

    withTestAiConnectionString(dto?: Raven.Server.Web.System.NodeConnectionTestResult) {
        return this.mockResolvedValue(
            this.mocks.testAiConnectionString,
            dto,
            SharedStubs.nodeConnectionTestSuccessResult()
        );
    }

    withTestGenAi() {
        return this.mocks.testGenAi.mockImplementation(async (_, dto) => {
            if (dto.TestStage === "CreateContextObjects") {
                return TasksStubs.testGenAiResults_context();
            }

            if (dto.TestStage === "SendToModel") {
                return TasksStubs.testGenAiResults_model();
            }

            if (dto.TestStage === "ApplyUpdateScript") {
                return TasksStubs.testGenAiResults_update();
            }
        });
    }

    withAiModels(dto?: MockedValue<string[]>) {
        return this.mockResolvedValue(this.mocks.getAiModels, dto, SharedStubs.aiModels());
    }

    withEtlErrors(dto?: MockedValue<EtlErrors[]>) {
        return this.mockResolvedValue(this.mocks.getEtlErrors, dto, TasksStubs.etlErrors());
    }

    withEtlStats(dto?: MockedValue<EtlTaskStats[]>) {
        return this.mockResolvedValue(this.mocks.getEtlStats, dto, TasksStubs.etlStats());
    }
}
