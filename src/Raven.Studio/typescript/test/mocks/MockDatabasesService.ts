import { AutoMockService, MockedValue } from "./AutoMockService";
import DatabasesService from "../../components/services/DatabasesService";
import { IndexesStubs } from "../stubs/IndexesStubs";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { DatabaseStubs } from "../DatabaseStubs";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import DatabasesInfo = Raven.Client.ServerWide.Operations.DatabasesInfo;

export default class MockDatabasesService extends AutoMockService<DatabasesService> {
    constructor() {
        super(new DatabasesService());
    }

    withEssentialStats(dto?: MockedValue<EssentialDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getEssentialStats, dto, DatabaseStubs.essentialStats());
    }

    withDetailedStats(dto?: MockedValue<DetailedDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getDetailedStats, dto, DatabaseStubs.detailedStats());
    }

    withGetDatabasesSharded(dto?: MockedValue<DatabasesInfo>) {
        return this.mockResolvedValue(this.mocks.getDatabases, dto, DatabaseStubs.shardedDatabasesResponse());
    }

    withGetDatabasesSingle(dto?: MockedValue<DatabasesInfo>) {
        return this.mockResolvedValue(this.mocks.getDatabases, dto, DatabaseStubs.singleDatabaseResponse());
    }
}
