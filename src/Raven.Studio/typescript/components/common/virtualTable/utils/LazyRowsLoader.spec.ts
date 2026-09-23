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

    const loader = new LazyRowsLoader<string>({
        fetchMode,
        minFetchCount,
        fetchData: (skip, take, continuationToken) =>
            new Promise((resolve, reject) => fetches.push({ skip, take, continuationToken, resolve, reject })),
    });

    return { loader, fetches };
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
            const { loader, fetches } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            loader.reset(true);

            expect(fetches).toHaveLength(2);

            await resolveFetch(fetches[0], { totalResultCount: 5 });

            expect(loader.getSnapshot().totalCount).toBeNull();
            expect(loader.getRows()).toHaveLength(0);
            expect(loader.getSnapshot().isFetching).toBe(true);
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

        it("stores a failed fetch and retries only on the next range request", async () => {
            const { loader, fetches } = createLoader();

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });

            const error = new Error("failed");
            fetches[0].reject(error);
            await flush();

            expect(loader.getSnapshot()).toMatchObject({ isFetching: false, error });
            expect(fetches).toHaveLength(1);

            loader.setRange({ start: 0, end: 10 });

            expect(fetches).toHaveLength(2);
            expect(loader.getSnapshot().error).toBeNull();
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

        it("fetches a range beyond the loaded rows by skip only when skipping is allowed", async () => {
            const { loader, fetches } = createLoader("continuationToken", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { continuationToken: "token-1" });

            loader.setRange({ start: 500, end: 520 }, { allowSkip: true });

            expect(fetches[1]).toMatchObject({ skip: 500, take: 20, continuationToken: undefined });

            await resolveFetch(fetches[1], { continuationToken: "ignored" });

            expect(loader.getSnapshot().loadedCount).toBe(10);
            expect(loader.getRows().map((x) => x.index)).toEqual(Array.from({ length: 20 }, (_, i) => 500 + i));

            loader.setRange({ start: 10, end: 20 }, { allowSkip: true });

            expect(fetches[2]).toMatchObject({ skip: 10, take: 10, continuationToken: "token-1" });
        });

        it("shows only the sequentially loaded rows when skipping is not allowed", async () => {
            const { loader, fetches } = createLoader("continuationToken", 10);

            loader.reset(true);
            loader.setRange({ start: 0, end: 10 });
            await resolveFetch(fetches[0], { continuationToken: "token-1" });

            loader.setRange({ start: 30, end: 40 }, { allowSkip: true });
            await resolveFetch(fetches[1]);

            loader.setRange({ start: 0, end: 40 });

            expect(fetches[2]).toMatchObject({ skip: 10, continuationToken: "token-1" });
            expect(loader.getRows()).toHaveLength(10);
        });
    });
});
