import { AutoMockService, MockedValue } from "./AutoMockService";
import TasksService from "../../components/services/TasksService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { TasksStubs } from "../stubs/TasksStubs";
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
}
