import ManageServerService from "components/services/ManageServerService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import ClientConfiguration = Raven.Client.Documents.Operations.Configuration.ClientConfiguration;
import { ManageServerStubs } from "test/stubs/ManageServerStubs";
import AnalyzerDefinition = Raven.Client.Documents.Indexes.Analysis.AnalyzerDefinition;
import SorterDefinition = Raven.Client.Documents.Queries.Sorting.SorterDefinition;

export default class MockManageServerService extends AutoMockService<ManageServerService> {
    constructor() {
        super(new ManageServerService());
    }

    withGetGlobalClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getGlobalClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientGlobalConfiguration()
        );
    }

    withThrowingGetGlobalClientConfiguration() {
        this.mocks.getGlobalClientConfiguration.mockRejectedValue(new Error());
    }

    withGetDatabaseClientConfiguration(dto?: MockedValue<ClientConfiguration>) {
        return this.mockResolvedValue(
            this.mocks.getClientConfiguration,
            dto,
            ManageServerStubs.getSampleClientDatabaseConfiguration()
        );
    }

    withGetServerWideCustomAnalyzers(dto?: MockedValue<AnalyzerDefinition[]>) {
        return this.mockResolvedValue(
            this.mocks.getServerWideCustomAnalyzers,
            dto,
            ManageServerStubs.getServerWideCustomAnalyzers()
        );
    }

    withThrowingGetServerWideCustomAnalyzers() {
        this.mocks.getServerWideCustomAnalyzers.mockRejectedValue(new Error());
    }

    withGetServerWideCustomSorters(dto?: MockedValue<SorterDefinition[]>) {
        return this.mockResolvedValue(
            this.mocks.getServerWideCustomSorters,
            dto,
            ManageServerStubs.getServerWideCustomSorters()
        );
    }

    withThrowingGetServerWideCustomSorters() {
        this.mocks.getServerWideCustomSorters.mockRejectedValue(new Error());
    }
}
