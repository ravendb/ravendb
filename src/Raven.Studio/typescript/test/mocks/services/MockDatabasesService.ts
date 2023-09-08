import { AutoMockService, MockedValue } from "./AutoMockService";
import DatabasesService from "components/services/DatabasesService";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import { DatabasesStubs } from "test/stubs/DatabasesStubs";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import RefreshConfiguration = Raven.Client.Documents.Operations.Refresh.RefreshConfiguration;
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;

interface WithGetDatabasesStateOptions {
    loadError?: string[];
    offlineNodes?: string[];
}

export default class MockDatabasesService extends AutoMockService<DatabasesService> {
    constructor() {
        super(new DatabasesService());
    }

    withGetDatabasesState(
        databaseListProvider: (nodeTag: string) => string[],
        options: WithGetDatabasesStateOptions = {}
    ) {
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

                if ((options.offlineNodes || []).includes(tag)) {
                    state.UpTime = null;
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

    withExpirationConfiguration(dto?: RefreshConfiguration) {
        return this.mockResolvedValue(
            this.mocks.getExpirationConfiguration,
            dto,
            DatabasesStubs.expirationConfiguration()
        );
    }

    withTombstonesState(dto?: TombstonesStateOnWire) {
        return this.mockResolvedValue(this.mocks.getTombstonesState, dto, DatabasesStubs.tombstonesState());
    }

    withRevisionsConfiguration(dto?: MockedValue<RevisionsConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getRevisionsConfiguration,
            dto,
            DatabasesStubs.revisionsConfiguration()
        );
    }

    withRevisionsForConflictsConfiguration(dto?: RevisionsCollectionConfiguration) {
        return this.mockResolvedValue(
            this.mocks.getRevisionsForConflictsConfiguration,
            dto,
            DatabasesStubs.revisionsForConflictsConfiguration()
        );
    }

    withCustomAnalyzers(dto?: MockedValue<AnalyzerDefinition[]>) {
        return this.mockResolvedValue(this.mocks.getCustomAnalyzers, dto, DatabasesStubs.customAnalyzers());
    }

    withCustomSorters(dto?: MockedValue<SorterDefinition[]>) {
        return this.mockResolvedValue(this.mocks.getCustomSorters, dto, DatabasesStubs.customSorters());
    }
}
