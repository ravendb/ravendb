import { LazyFetchMode, LazyRowsLoader } from "./LazyRowsLoader";

interface PendingFetch {
    skip: number;
    take: number;
    continuationToken: string | undefined;
    resolve: (result: pagedResultWithToken<string>) => void;
    reject: (error: unknown) => void;
}

function createLoader(fetchMode: LazyFetchMode = "skipTake", minFetchCount = 10) {
    const fetches: PendingFetch[] = [];
    const onResult = jest.fn();

    const loader = new LazyRowsLoader<string>({
        fetchMode,
        minFetchCount,
        onResult,
        fetchData: (skip, take, continuationToken) =>
            new Promise((resolve, reject) => fetches.push({ skip, take, continuationToken, resolve, reject })),
    });

    return { loader, fetches, onResult };
}

function createItems(skip: number, count: number) {
    return Array.from({ length: count }, (_, i) => `Item ${skip + i}`);
}

async function resolveFetch(
    fetch: PendingFetch,
    {
        count = fetch.take,
        totalResultCount = 1000,
        continuationToken,
    }: Partial<{
        count: number;
        totalResultCount: number;
        continuationToken: string;
    }> = {}
) {
    fetch.resolve({ items: createItems(fetch.skip, count), totalResultCount, continuationToken });
    await flush();
}

const flush = () => new Promise(process.nextTick);

describe("LazyRowsLoader", () => {
    describe("skipTake mode", () => {
        it("fetches the missing rows of the range with at least the minimal fetch count", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 5 });

            expect(fetches).toHaveLength(1);
            expect(fetches[0]).toMatchObject({ skip: 0, take: 10, continuationToken: undefined });
            expect(loader.getSnapshot().isFetching).toBe(true);

            await resolveFetch(fetches[0]);

            const snapshot = loader.getSnapshot();
            expect(snapshot.isFetching).toBe(false);
            expect(snapshot.totalCount).toBe(1000);
            expect(loader.getRows().map((x) => x.item)).toEqual(createItems(0, 5));
            expect(loader.getRows().map((x) => x.index)).toEqual([0, 1, 2, 3, 4]);

            loader.setRange({ start: 5, end: 10 });
            expect(fetches).toHaveLength(1);

            loader.setRange({ start: 8, end: 30 });
            expect(fetches[1]).toMatchObject({ skip: 10, take: 20 });
        });

        it("fixes the total count to the loaded end when a batch comes back short", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { count: 4, totalResultCount: 1000 });

            expect(loader.getSnapshot().totalCount).toBe(4);
            expect(loader.getRows()).toHaveLength(4);
        });

        it("keeps a single request in flight and fetches the range requested meanwhile afterwards", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            loader.setRange({ start: 50, end: 60 });

            expect(fetches).toHaveLength(1);

            await resolveFetch(fetches[0]);

            expect(fetches).toHaveLength(2);
            expect(fetches[1]).toMatchObject({ skip: 50, take: 10 });
        });

        it("does not fetch before the first reset", async () => {
            const { loader, fetches } = createLoader();

            loader.setRange({ start: 0, end: 10 });
            expect(fetches).toHaveLength(0);

            loader.reset(true);
            expect(fetches).toHaveLength(1);
            expect(fetches[0]).toMatchObject({ skip: 0, take: 10 });
        });

        it("ignores a result that arrives after a reset", async () => {
            const { loader, fetches, onResult } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            loader.reset(true);

            expect(fetches).toHaveLength(2);

            await resolveFetch(fetches[0], { totalResultCount: 5 });

            expect(loader.getSnapshot().totalCount).toBeNull();
            expect(loader.getRows()).toHaveLength(0);
            expect(loader.getSnapshot().isFetching).toBe(true);
            expect(onResult).not.toHaveBeenCalled();

            await resolveFetch(fetches[1], { totalResultCount: 7 });

            expect(onResult).toHaveBeenCalledTimes(1);
            expect(onResult).toHaveBeenCalledWith(expect.objectContaining({ totalResultCount: 7 }));
        });

        it("keeps the total count and the range on a soft reset and moves to the first row on a hard one", async () => {
            const { loader, fetches } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 20, end: 30 });
            await resolveFetch(fetches[0]);

            loader.reset(false);

            expect(loader.getSnapshot()).toMatchObject({ totalCount: 1000, range: { start: 20, end: 30 }, resetId: 1 });
            expect(fetches[1]).toMatchObject({ skip: 20 });

            await resolveFetch(fetches[1]);
            loader.reset(true);

            expect(loader.getSnapshot()).toMatchObject({ totalCount: null, range: { start: 0, end: 10 }, resetId: 2 });
            expect(fetches[2]).toMatchObject({ skip: 0 });
        });

        it("treats a negative total count as unknown", async () => {
            const { loader, fetches } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { totalResultCount: -1 });

            expect(loader.getSnapshot().totalCount).toBe(10);

            const tokenLoader = createLoader("continuationToken");
            tokenLoader.loader.reset(true);
            tokenLoader.loader.setRange({ start: 0, end: 10 });
            await resolveFetch(tokenLoader.fetches[0], { totalResultCount: -1, continuationToken: "10" });

            expect(tokenLoader.loader.getSnapshot().totalCount).toBeNull();
        });

        it("keeps the error of a failed fetch until a retry or another range request", async () => {
            const { loader, fetches } = createLoader();
            const error = new Error("failed");

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });

            fetches[0].reject(error);
            await flush();

            expect(loader.getSnapshot()).toMatchObject({ isFetching: false, error });

            loader.setRange({ start: 0, end: 10 });
            expect(fetches).toHaveLength(1);

            loader.retry();
            expect(loader.getSnapshot().error).toBeNull();
            expect(fetches).toHaveLength(2);

            fetches[1].reject(error);
            await flush();

            loader.setRange({ start: 10, end: 20 });
            expect(loader.getSnapshot().error).toBeNull();
            expect(fetches).toHaveLength(3);
        });

        it("fetches backward only the rows between the loaded ones", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0]);

            loader.setRange({ start: 15, end: 25 });
            expect(fetches[1]).toMatchObject({ skip: 15, take: 10 });
            await resolveFetch(fetches[1]);

            const loadedItem = loader.getItem(5);

            loader.setRange({ start: 0, end: 25 });
            expect(fetches[2]).toMatchObject({ skip: 10, take: 5 });

            fetches[2].resolve({ items: createItems(10, 5).map((x) => `${x} (new)`), totalResultCount: 1000 });
            await flush();

            expect(loader.getItem(5)).toBe(loadedItem);
            expect(loader.getItem(12)).toBe("Item 12 (new)");
        });

        it("stops the forward fetch at the loaded rows", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0]);

            loader.setRange({ start: 20, end: 30 });
            await resolveFetch(fetches[1]);

            loader.setRange({ start: 12, end: 17 });
            expect(fetches[2]).toMatchObject({ skip: 12, take: 8 });
        });

        it("drops the rows far from the range once the cache is full and fetches them again when needed", async () => {
            const { loader, fetches } = createLoader("skipTake", 10);
            const totalResultCount = 100_000;

            loader.reset(true);
            loader.setRange({ start: 0, end: 6000 });
            await resolveFetch(fetches[0], { totalResultCount });

            loader.setRange({ start: 50_000, end: 56_000 });
            await resolveFetch(fetches[1], { totalResultCount });

            expect(loader.getItem(0)).toBe("Item 0");

            loader.setRange({ start: 50_000, end: 50_010 });

            expect(loader.getItem(0)).toBeUndefined();
            expect(loader.getItem(50_000)).toBe("Item 50000");
            expect(fetches).toHaveLength(2);

            loader.setRange({ start: 0, end: 10 });

            expect(fetches[2]).toMatchObject({ skip: 0, take: 10 });
        });

        it("drops an in flight result after cancel", async () => {
            const { loader, fetches } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            loader.cancel();

            expect(loader.getSnapshot().isFetching).toBe(false);

            await resolveFetch(fetches[0]);

            expect(loader.getRows()).toHaveLength(0);
            expect(fetches).toHaveLength(1);
        });

        it("notifies the subscribers with a new snapshot on every change", async () => {
            const { loader, fetches } = createLoader();
            const listener = jest.fn();
            const unsubscribe = loader.subscribe(listener);

            const initialSnapshot = loader.getSnapshot();
            expect(loader.getSnapshot()).toBe(initialSnapshot);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0]);

            expect(listener).toHaveBeenCalled();
            expect(loader.getSnapshot()).not.toBe(initialSnapshot);

            unsubscribe();
            listener.mockClear();
            loader.reset(true);

            expect(listener).not.toHaveBeenCalled();
        });
    });

    describe("continuationToken mode", () => {
        it("appends sequential batches using the returned token until there is no more", async () => {
            const { loader, fetches } = createLoader("continuationToken", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 25 });

            expect(fetches[0]).toMatchObject({ skip: 0, take: 10, continuationToken: undefined });

            await resolveFetch(fetches[0], { continuationToken: "token-1" });

            expect(fetches[1]).toMatchObject({ skip: 10, take: 10, continuationToken: "token-1" });

            await resolveFetch(fetches[1], { continuationToken: "token-2" });

            expect(fetches[2]).toMatchObject({ skip: 20, take: 10, continuationToken: "token-2" });

            await resolveFetch(fetches[2], { count: 5 });

            expect(loader.getSnapshot()).toMatchObject({ loadedCount: 25, hasMore: false, isFetching: false });
            expect(loader.getRows()).toHaveLength(25);

            loader.setRange({ start: 0, end: 40 });

            expect(fetches).toHaveLength(3);
        });

        it("loads sequentially up to a range beyond the loaded rows", async () => {
            const { loader, fetches } = createLoader("continuationToken", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { continuationToken: "token-1" });

            loader.setRange({ start: 30, end: 40 });

            expect(fetches[1]).toMatchObject({ skip: 10, continuationToken: "token-1" });
            expect(loader.getRows()).toHaveLength(0);
        });

        it("keeps the rows far from the range since they cannot be fetched by offset", async () => {
            const { loader, fetches } = createLoader("continuationToken", 6000);
            const totalResultCount = 100_000;

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { totalResultCount, continuationToken: "token-1" });

            loader.setRange({ start: 11_990, end: 12_000 });
            await resolveFetch(fetches[1], { totalResultCount, continuationToken: "token-2" });

            loader.setRange({ start: 11_000, end: 11_010 });

            expect(loader.getRows()[0]).toEqual({ index: 11_000, item: "Item 11000" });
            expect(loader.getItem(0)).toBe("Item 0");
        });

        it("starts from the first row on a soft reset", async () => {
            const { loader, fetches } = createLoader("continuationToken", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 20 });
            await resolveFetch(fetches[0], { continuationToken: "token-1" });
            await resolveFetch(fetches[1], { continuationToken: "token-2" });

            loader.setRange({ start: 10, end: 20 });
            loader.reset(false);

            expect(loader.getSnapshot()).toMatchObject({ range: { start: 0, end: 10 }, resetId: 2 });
            expect(fetches[2]).toMatchObject({ skip: 0, continuationToken: undefined });
        });
    });
});
