import { AutoMockService, MockedValue } from "./AutoMockService";
import TasksService from "components/services/TasksService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { TasksStubs } from "test/stubs/TasksStubs";
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import GetPeriodicBackupStatusOperationResult = Raven.Client.Documents.Operations.Backups.GetPeriodicBackupStatusOperationResult;

export default class MockTasksService extends AutoMockService<TasksService> {
    constructor() {
        super(new TasksService());
    }

    withGetTasks(dto?: MockedValue<OngoingTasksResult>) {
        return this.mockResolvedValue(this.mocks.getOngoingTasks, dto, TasksStubs.getTasksList());
    }

    withGetProgress(dto?: MockedValue<resultsDto<EtlTaskProgress>>) {
        return this.mockResolvedValue(this.mocks.getProgress, dto, TasksStubs.getTasksProgress());
    }

    withGetManualBackup(dto?: MockedValue<GetPeriodicBackupStatusOperationResult>) {
        return this.mockResolvedValue(this.mocks.getManualBackup, dto, TasksStubs.getManualBackup());
    }

    withGetSubscriptionTaskInfo(
        dto?: MockedValue<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription>
    ) {
        return this.mockResolvedValue(this.mocks.getSubscriptionTaskInfo, dto, TasksStubs.subscriptionTaskInfo());
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
}
