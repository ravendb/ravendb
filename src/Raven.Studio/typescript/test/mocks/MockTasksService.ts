import { AutoMockService, MockedValue } from "./AutoMockService";
import DatabasesService from "../../components/services/DatabasesService";
import { IndexesStubs } from "../stubs/IndexesStubs";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { DatabaseStubs } from "../DatabaseStubs";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import DatabasesInfo = Raven.Client.ServerWide.Operations.DatabasesInfo;
import TasksService from "../../components/services/TasksService";

export default class MockTasksService extends AutoMockService<TasksService> {
    constructor() {
        super(new TasksService());
    }
}
