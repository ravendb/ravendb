import { AutoMockService, MockedValue } from "./AutoMockService";
import DatabasesService from "components/services/DatabasesService";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import DatabasesInfo = Raven.Client.ServerWide.Operations.DatabasesInfo;
import DatabaseInfo = Raven.Client.ServerWide.Operations.DatabaseInfo;
import { DatabasesStubs } from "test/stubs/DatabasesStubs";

export default class MockDatabasesService extends AutoMockService<DatabasesService> {
    constructor() {
        super(new DatabasesService());
    }

    withEssentialStats(dto?: MockedValue<EssentialDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getEssentialStats, dto, DatabasesStubs.essentialStats());
    }

    withDetailedStats(dto?: MockedValue<DetailedDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getDetailedStats, dto, DatabasesStubs.detailedStats());
    }

    withGetDatabases_Sharded(dto?: MockedValue<DatabasesInfo>) {
        const defaultResponse: DatabasesInfo = {
            Databases: DatabasesStubs.shardedDatabaseDto(),
        };
        return this.mockResolvedValue(this.mocks.getDatabases, dto, defaultResponse);
    }

    withGetDatabases_Single(dto?: MockedValue<DatabasesInfo>) {
        const defaultResponse: DatabasesInfo = {
            Databases: [DatabasesStubs.nonShardedSingleNodeDatabaseDto()],
        };
        return this.mockResolvedValue(this.mocks.getDatabases, dto, defaultResponse);
    }

    withGetDatabase_Single(dto?: MockedValue<DatabaseInfo>) {
        return this.mockResolvedValue(this.mocks.getDatabase, dto, DatabasesStubs.nonShardedSingleNodeDatabaseDto());
    }

    withGetDatabase_Cluster(dto?: MockedValue<DatabaseInfo>) {
        return this.mockResolvedValue(this.mocks.getDatabase, dto, DatabasesStubs.nonShardedClusterDatabaseDto());
    }
}
