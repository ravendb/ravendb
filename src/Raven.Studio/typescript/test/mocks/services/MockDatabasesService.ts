import { AutoMockService, MockedValue } from "./AutoMockService";
import DatabasesService from "components/services/DatabasesService";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;

export default class MockDatabasesService extends AutoMockService<DatabasesService> {
    constructor() {
        super(new DatabasesService());
    }

    withGetDatabasesState(databaseListProvider: (nodeTag: string) => string[], options: { loadError?: string[] } = {}) {
        this.mocks.getDatabasesState.mockImplementation(async (tag) => {
            const dbs = databaseListProvider(tag);

            const dtos = dbs.map((db): StudioDatabaseState => {
                const state = DatabasesStubs.studioDatabaseState(db);

                if ((options.loadError || []).includes(tag)) {
                    return {
                        Name: db,
                        LoadError: "This is some load error!",
                    } as StudioDatabaseState;
                }

                return state;
            });

            return {
                Databases: dtos,
                Orchestrators: [],
            };
        });
    }

    withEssentialStats(dto?: MockedValue<EssentialDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getEssentialStats, dto, DatabasesStubs.essentialStats());
    }

    withDetailedStats(dto?: MockedValue<DetailedDatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getDetailedStats, dto, DatabasesStubs.detailedStats());
    }

    withRefreshConfiguration(dto?: RefreshConfiguration) {
        return this.mockResolvedValue(this.mocks.getRefreshConfiguration, dto, DatabasesStubs.refreshConfiguration());
    }
}
