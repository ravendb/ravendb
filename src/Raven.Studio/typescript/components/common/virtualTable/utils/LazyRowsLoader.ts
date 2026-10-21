import { findMissingRange, isSameRange, RowRange } from "./lazyTableUtils";

export type LazyFetchMode = "skipTake" | "continuationToken";

export type LazyFetchData<T, TResult extends pagedResultWithToken<T> = pagedResultWithToken<T>> = (
    skip: number,
    take: number,
    continuationToken?: string
) => Promise<TResult>;

export interface LazyRow<T> {
    index: number;
    item: T;
}

interface LazyRowsLoaderOptions<T, TResult extends pagedResultWithToken<T>> {
    fetchData: LazyFetchData<T, TResult>;
    fetchMode: LazyFetchMode;
    minFetchCount: number;
    onResult?: (result: TResult) => void;
    onReset?: () => void;
}

interface LazyRowsSnapshot {
    range: RowRange;
    version: number;
    resetId: number;
    totalCount: number | null;
    loadedCount: number;
    hasMore: boolean;
    isFetching: boolean;
    error: unknown;
}

interface FetchRequest {
    skip: number;
    take: number;
    continuationToken?: string;
    isSequential: boolean;
}

const MAX_CACHED_ROWS = 10_000;

const INITIAL_SNAPSHOT: LazyRowsSnapshot = {
    range: { start: 0, end: 0 },
    version: 0,
    resetId: 0,
    totalCount: null,
    loadedCount: 0,
    hasMore: true,
    isFetching: false,
    error: null,
};

export class LazyRowsLoader<T, TResult extends pagedResultWithToken<T> = pagedResultWithToken<T>> {
    private items = new Map<number, T>();
    private continuationToken: string | undefined;
    private generation = 0;
    private isStarted = false;
    private snapshot = INITIAL_SNAPSHOT;
    private listeners = new Set<() => void>();

    constructor(private options: LazyRowsLoaderOptions<T, TResult>) {}

    setOptions(options: LazyRowsLoaderOptions<T, TResult>) {
        this.options = options;
    }

    subscribe = (listener: () => void) => {
        this.listeners.add(listener);
        return () => {
            this.listeners.delete(listener);
        };
    };

    getSnapshot = () => this.snapshot;

    getItem = (rowIndex: number): T | undefined => this.items.get(rowIndex);

    getRows(): LazyRow<T>[] {
        const { range, loadedCount, totalCount } = this.snapshot;
        const limit = this.options.fetchMode === "continuationToken" ? loadedCount : (totalCount ?? Infinity);
        const end = Math.min(range.end, limit);

        const rows: LazyRow<T>[] = [];
        for (let i = range.start; i < end; i++) {
            const item = this.items.get(i);
            if (item !== undefined) {
                rows.push({ index: i, item });
            }
        }

        return rows;
    }

    setRange = (range: RowRange) => {
        if (!isSameRange(range, this.snapshot.range)) {
            this.evictRowsAwayFrom(range);
            this.update({ range, error: null });
        }

        this.load();
    };

    reset = (isHard: boolean) => {
        this.generation++;
        this.items = new Map();
        this.continuationToken = undefined;
        this.isStarted = true;
        this.options.onReset?.();

        const { range, version, resetId } = this.snapshot;
        const isRangeRestarted = isHard || this.options.fetchMode === "continuationToken";

        this.update({
            version: version + 1,
            loadedCount: 0,
            hasMore: true,
            isFetching: false,
            error: null,
            ...(isRangeRestarted && {
                range: { start: 0, end: range.end - range.start },
                resetId: resetId + 1,
                totalCount: null,
            }),
        });

        this.load();
    };

    retry = () => {
        this.update({ error: null });
        this.load();
    };

    cancel = () => {
        this.generation++;
        this.update({ isFetching: false });
    };

    private async load() {
        if (!this.isStarted || this.snapshot.isFetching || this.snapshot.error) {
            return;
        }

        const request = this.getNextRequest();
        if (!request) {
            return;
        }

        const generation = this.generation;
        this.update({ isFetching: true });

        let result: TResult;
        try {
            result = await this.options.fetchData(request.skip, request.take, request.continuationToken);
        } catch (error) {
            if (generation === this.generation) {
                this.update({ isFetching: false, error: error ?? "Failed to fetch the rows" });
            }
            return;
        }

        if (generation !== this.generation) {
            return;
        }

        this.apply(request, result);
        this.options.onResult?.(result);
        this.load();
    }

    private getNextRequest(): FetchRequest | null {
        const { range, totalCount, loadedCount, hasMore } = this.snapshot;
        const { fetchMode, minFetchCount } = this.options;
        const end = Math.min(range.end, totalCount ?? Infinity);

        if (range.start >= end) {
            return null;
        }

        if (fetchMode === "continuationToken") {
            if (!hasMore || end <= loadedCount) {
                return null;
            }

            return {
                skip: loadedCount,
                take: minFetchCount,
                continuationToken: this.continuationToken,
                isSequential: true,
            };
        }

        const missing = findMissingRange({ start: range.start, end }, (i) => this.items.has(i));
        if (!missing) {
            return null;
        }

        if (this.items.has(missing.end)) {
            const minSkip = Math.max(0, missing.end - minFetchCount);
            let skip = missing.start;
            while (skip > minSkip && !this.items.has(skip - 1)) {
                skip--;
            }

            return { skip, take: missing.end - skip, isSequential: false };
        }

        const maxEnd = Math.min(
            missing.start + Math.max(minFetchCount, missing.end - missing.start),
            totalCount ?? Infinity
        );
        let fetchEnd = missing.end;
        while (fetchEnd < maxEnd && !this.items.has(fetchEnd)) {
            fetchEnd++;
        }

        return { skip: missing.start, take: fetchEnd - missing.start, isSequential: false };
    }

    private apply({ skip, take, isSequential }: FetchRequest, result: pagedResultWithToken<T>) {
        result.items.forEach((item, i) => {
            if (!this.items.has(skip + i)) {
                this.items.set(skip + i, item);
            }
        });

        const loadedEnd = skip + result.items.length;
        const reportedTotal = result.totalResultCount >= 0 ? result.totalResultCount : null;
        const version = this.snapshot.version + 1;

        if (isSequential) {
            this.continuationToken = result.continuationToken;
            this.update({
                version,
                loadedCount: loadedEnd,
                hasMore: !!result.continuationToken && result.items.length > 0,
                totalCount: reportedTotal,
                isFetching: false,
            });
            return;
        }

        const nextTotalCount = reportedTotal ?? this.snapshot.totalCount ?? loadedEnd;
        const isLastBatch = result.items.length < take;

        this.update({
            version,
            totalCount: isLastBatch ? Math.min(nextTotalCount, loadedEnd) : nextTotalCount,
            isFetching: false,
        });
    }

    private evictRowsAwayFrom({ start, end }: RowRange) {
        const canFetchRowsAgain = this.options.fetchMode === "skipTake";
        if (!canFetchRowsAgain || this.items.size <= MAX_CACHED_ROWS) {
            return;
        }

        const retainedRowsAroundRange = MAX_CACHED_ROWS / 2;

        for (const rowIndex of this.items.keys()) {
            if (rowIndex < start - retainedRowsAroundRange || rowIndex >= end + retainedRowsAroundRange) {
                this.items.delete(rowIndex);
            }
        }
    }

    private update(changes: Partial<LazyRowsSnapshot>) {
        this.snapshot = { ...this.snapshot, ...changes };
        this.listeners.forEach((listener) => listener());
    }
}
