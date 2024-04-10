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
import DataArchival = Raven.Client.Documents.Operations.DataArchival.DataArchivalConfiguration;
import document from "models/database/documents/document";

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

    withDataArchivalConfiguration(dto?: DataArchival) {
        return this.mockResolvedValue(
            this.mocks.getDataArchivalConfiguration,
            dto,
            DatabasesStubs.dataArchivalConfiguration()
        );
    }

    withDocumentsCompressionConfiguration(dto?: Raven.Client.ServerWide.DocumentsCompressionConfiguration) {
        return this.mockResolvedValue(
            this.mocks.getDocumentsCompressionConfiguration,
            dto,
            DatabasesStubs.documentsCompressionConfiguration()
        );
    }

    withConflictSolverConfiguration(dto?: Raven.Client.ServerWide.ConflictSolver) {
        return this.mockResolvedValue(
            this.mocks.getConflictSolverConfiguration,
            dto,
            DatabasesStubs.conflictSolverConfiguration()
        );
    }

    withDatabaseRecord(dto?: MockedValue<document>) {
        return this.mockResolvedValue(this.mocks.getDatabaseRecord, dto, DatabasesStubs.databaseRecord());
    }

    withQueryResult(dto?: MockedValue<pagedResultExtended<document>>) {
        return this.mockResolvedValue(this.mocks.query, dto, DatabasesStubs.queryResult());
    }

    withIntegrationsPostgreSqlSupport(isActive?: boolean) {
        return this.mockResolvedValue(
            this.mocks.getIntegrationsPostgreSqlSupport,
            { Active: isActive },
            { Active: true }
        );
    }

    withIntegrationsPostgreSqlCredentials(
        dto?: MockedValue<Raven.Server.Integrations.PostgreSQL.Handlers.PostgreSqlUsernames>
    ) {
        return this.mockResolvedValue(
            this.mocks.getIntegrationsPostgreSqlCredentials,
            dto,
            DatabasesStubs.integrationsPostgreSqlCredentials()
        );
    }

    withGenerateSecret(secret?: MockedValue<string>) {
        return this.mockResolvedValue(
            this.mocks.generateSecret,
            secret,
            "MXEv4ntxod7qM4mOeF9YZlKIuar1RKU8yQcQSESCzys="
        );
    }

    withDatabaseStats(dto?: MockedValue<Raven.Client.Documents.Operations.DatabaseStatistics>) {
        return this.mockResolvedValue(this.mocks.getDatabaseStats, dto, DatabasesStubs.detailedStats());
    }

    withDocumentsMetadataByIDPrefix(dto?: MockedValue<metadataAwareDto[]>) {
        return this.mockResolvedValue(
            this.mocks.getDocumentsMetadataByIDPrefix,
            dto,
            DatabasesStubs.documentsMetadataByIDPrefix()
        );
    }
}
