import collectionsTracker from "common/helpers/database/collectionsTracker";
import getCollectionsStatsCommand from "commands/database/documents/getCollectionsStatsCommand";
import getRevisionsPreviewCommand from "commands/database/documents/getRevisionsPreviewCommand";
import collectionsStats from "models/database/documents/collectionsStats";
import database from "models/resources/database";
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { createStoreConfiguration } from "components/store";
import { setEffectiveTestStore } from "components/storeCompat";
import CollectionStatistics = Raven.Client.Documents.Operations.CollectionStatistics;

function createTracker() {
    const store = createStoreConfiguration();
    setEffectiveTestStore(store);

    const statsTasks: JQueryDeferred<collectionsStats>[] = [];
    jest.spyOn(getCollectionsStatsCommand.prototype, "execute").mockImplementation(() => {
        const task = $.Deferred<collectionsStats>();
        statsTasks.push(task);
        return task;
    });
    jest.spyOn(getRevisionsPreviewCommand.prototype, "execute").mockReturnValue($.Deferred());

    return { tracker: new collectionsTracker(), statsTasks, getState: store.getState };
}

const createDatabase = (name: string) => ({ name }) as database;

const createStats = () => {
    const dto: Pick<CollectionStatistics, "Collections" | "CountOfDocuments" | "CountOfConflicts"> = {
        Collections: { Orders: 5 },
        CountOfDocuments: 5,
        CountOfConflicts: 0,
    };

    return new collectionsStats(dto as CollectionStatistics);
};

describe("collectionsTracker", () => {
    afterEach(() => {
        jest.restoreAllMocks();
    });

    it("reports the failed stats load of the current database until the load is retried", () => {
        const { tracker, statsTasks, getState } = createTracker();

        tracker.onDatabaseChanged(createDatabase("db1"));
        statsTasks[0].reject();

        expect(collectionsTrackerSelectors.loadFailedDatabaseName(getState())).toBe("db1");

        tracker.reloadStats();

        expect(collectionsTrackerSelectors.loadFailedDatabaseName(getState())).toBeNull();

        statsTasks[1].resolve(createStats());

        expect(collectionsTrackerSelectors.databaseName(getState())).toBe("db1");
        expect(collectionsTrackerSelectors.collectionNames(getState())).toEqual(["Orders"]);
    });

    it("ignores the result of a stats load replaced by a newer one", () => {
        const { tracker, statsTasks, getState } = createTracker();
        const db1 = createDatabase("db1");

        tracker.onDatabaseChanged(db1);
        tracker.onDatabaseChanged(createDatabase("db2"));
        tracker.onDatabaseChanged(db1);

        statsTasks[0].reject();
        statsTasks[1].resolve(createStats());

        expect(collectionsTrackerSelectors.loadFailedDatabaseName(getState())).toBeNull();
        expect(collectionsTrackerSelectors.databaseName(getState())).toBeNull();

        statsTasks[2].resolve(createStats());

        expect(collectionsTrackerSelectors.databaseName(getState())).toBe("db1");
    });
});
