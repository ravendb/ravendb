import IndexesService from "../../components/services/IndexesService";
import { AutoMockService, MockedValue } from "./AutoMockService";
import { IndexesStubs } from "../stubs/IndexesStubs";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;

export default class MockIndexesService extends AutoMockService<IndexesService> {
    constructor() {
        super(new IndexesService());
    }

    withGetStats(dto?: MockedValue<IndexStats[]>) {
        return this.mockResolvedValue(this.mocks.getStats, dto, IndexesStubs.getSampleStats());
    }

    withGetSampleStats() {
        return this.withGetStats();
    }

    withGetProgress(dto?: MockedValue<IndexProgress[]>) {
        return this.mockResolvedValue(this.mocks.getProgress, dto, IndexesStubs.getSampleProgress());
    }
}
