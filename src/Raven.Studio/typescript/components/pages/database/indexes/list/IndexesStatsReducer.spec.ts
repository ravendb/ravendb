import { indexesStatsReducer, indexesStatsReducerInitializer } from "./IndexesStatsReducer";
import { IndexesStubs } from "test/stubs/IndexesStubs";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;

const location: databaseLocationSpecifier = { nodeTag: "A" };

type IndexesStatsState = ReturnType<typeof indexesStatsReducerInitializer>;

function staleIndex(name: string): [IndexStats, IndexProgress] {
    const [stats, progress] = IndexesStubs.getStaleInProgressIndex();
    stats.Name = name;
    progress.Name = name;
    return [stats, progress];
}

function withItemsToProcess(progress: IndexProgress, itemsToProcess: number): IndexProgress {
    const copy: IndexProgress = JSON.parse(JSON.stringify(progress));
    copy.Collections["Orders"].NumberOfItemsToProcess = itemsToProcess;
    return copy;
}

function createState() {
    const [statsA, progressA] = staleIndex("IndexA");
    const [statsB, progressB] = staleIndex("IndexB");

    const initial = indexesStatsReducerInitializer([location]);
    const withStats = indexesStatsReducer(initial, { type: "StatsLoaded", location, stats: [statsA, statsB] });
    const state = indexesStatsReducer(withStats, {
        type: "ProgressLoaded",
        location,
        progress: [progressA, progressB],
    });

    return { state, progressA, progressB };
}

function nodeInfo(state: IndexesStatsState, indexName: string) {
    return state.indexes.find((x) => x.name === indexName).nodesInfo[0];
}

function ordersProgress(state: IndexesStatsState, indexName: string) {
    return nodeInfo(state, indexName).progress.collections.find((x) => x.name === "Orders");
}

describe("IndexesStatsReducer", () => {
    describe("ProgressLoaded", () => {
        it("updates only the indexes in scope", () => {
            const { state, progressA, progressB } = createState();
            const progressOfBBefore = nodeInfo(state, "IndexB").progress;

            const next = indexesStatsReducer(state, {
                type: "ProgressLoaded",
                location,
                progress: [withItemsToProcess(progressA, 1), withItemsToProcess(progressB, 1)],
                scope: ["IndexA"],
            });

            const orders = ordersProgress(next, "IndexA");
            expect(orders.documents.processed).toBe(orders.documents.total - 1);
            expect(nodeInfo(next, "IndexB").progress).toBe(progressOfBBefore);
        });

        it("leaves the excluded indexes untouched", () => {
            const { state, progressA, progressB } = createState();
            const progressOfABefore = nodeInfo(state, "IndexA").progress;

            const next = indexesStatsReducer(state, {
                type: "ProgressLoaded",
                location,
                progress: [withItemsToProcess(progressA, 1), withItemsToProcess(progressB, 1)],
                exclude: ["IndexA"],
            });

            expect(nodeInfo(next, "IndexA").progress).toBe(progressOfABefore);
            const orders = ordersProgress(next, "IndexB");
            expect(orders.documents.processed).toBe(orders.documents.total - 1);
        });

        it("does not complete an excluded index when its progress is missing", () => {
            const { state } = createState();
            const before = nodeInfo(state, "IndexA");

            const next = indexesStatsReducer(state, {
                type: "ProgressLoaded",
                location,
                progress: [],
                exclude: ["IndexA"],
            });

            const after = nodeInfo(next, "IndexA");
            expect(after.progress).toBe(before.progress);
            expect(after.details.stale).toBe(true);
        });

        it("marks an index without progress as completed and clears the estimated flags", () => {
            const { state } = createState();
            expect(nodeInfo(state, "IndexA").progress.collections.some((x) => x.estimated)).toBe(true);

            const next = indexesStatsReducer(state, { type: "ProgressLoaded", location, progress: [] });

            const info = nodeInfo(next, "IndexA");
            expect(info.details.stale).toBe(false);
            expect(info.progress.global.processed).toBe(info.progress.global.total);
            info.progress.collections.forEach((collection) => {
                expect(collection.estimated).toBe(false);
                expect(collection.documents.processed).toBe(collection.documents.total);
                expect(collection.tombstones.processed).toBe(collection.tombstones.total);
                expect(collection.deletedTimeSeries.processed).toBe(collection.deletedTimeSeries.total);
            });
        });

        it("completes only the index in scope when its progress is missing", () => {
            const { state } = createState();
            const progressOfBBefore = nodeInfo(state, "IndexB").progress;

            const next = indexesStatsReducer(state, {
                type: "ProgressLoaded",
                location,
                progress: [],
                scope: ["IndexA"],
            });

            expect(nodeInfo(next, "IndexA").details.stale).toBe(false);
            expect(nodeInfo(next, "IndexB").details.stale).toBe(true);
            expect(nodeInfo(next, "IndexB").progress).toBe(progressOfBBefore);
        });
    });
});
