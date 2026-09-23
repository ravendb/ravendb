import { findMissingRange, isSameRange, RowRange } from "./lazyTableUtils";

export type LazyFetchMode = "skipTake" | "continuationToken";

export type LazyFetchData<T> = (
    skip: number,
    take: number,
    continuationToken?: string
) => Promise<pagedResultWithToken<T>>;

export interface LazyRow<T> {
    index: number;
    item: T;
}

export interface LazyRowsLoaderOptions<T> {
    fetchData: LazyFetchData<T>;
    fetchMode: LazyFetchMode;
    minFetchCount: number;
}

export interface LazyRowsSnapshot {
    range: RowRange;
    allowSkip: boolean;
    // bumps whenever the cached items change
    version: number;
    // bumps on every hard reset
    resetId: number;
    totalCount: number | null;
    // rows loaded one after another from the first one (continuationToken mode)
    loadedCount: number;
    hasMore: boolean;
    isFetching: boolean;
    error: unknown;
}

export interface SetRangeOptions {
    // continuationToken mode: rows beyond the sequentially loaded ones may be fetched by skip (pagination)
    allowSkip?: boolean;
}

interface FetchRequest {
    skip: number;
    take: number;
    continuationToken?: string;
    isSequential: boolean;
}

const initialSnapshot: LazyRowsSnapshot = {
    range: { start: 0, end: 0 },
    allowSkip: false,
    version: 0,
    resetId: 0,
    totalCount: null,
    loadedCount: 0,
    hasMore: true,
    isFetching: false,
    error: null,
};

export class LazyRowsLoader<T> {
    private options: LazyRowsLoaderOptions<T>;
    private items = new Map<number, T>();
    private continuationToken: string | undefined;
    private generation = 0;
    private isStarted = false;
    private snapshot = initialSnapshot;
    private listeners = new Set<() => void>();

    constructor(options: LazyRowsLoaderOptions<T>) {
        this.options = options;
    }

    setOptions(options: LazyRowsLoaderOptions<T>) {
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
        const { range, allowSkip, loadedCount, totalCount } = this.snapshot;
        const limit =
            this.options.fetchMode === "continuationToken" && !allowSkip ? loadedCount : (totalCount ?? Infinity);
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

    setRange = (range: RowRange, { allowSkip = false }: SetRangeOptions = {}) => {
        if (!isSameRange(range, this.snapshot.range) || allowSkip !== this.snapshot.allowSkip) {
            this.update({ range, allowSkip });
        }

        this.load();
    };

    reset = (isHard: boolean) => {
        this.generation++;
        this.items = new Map();
        this.continuationToken = undefined;
        this.isStarted = true;

        const { range, version, resetId, totalCount } = this.snapshot;

        this.update({
            version: version + 1,
            loadedCount: 0,
            hasMore: true,
            isFetching: false,
            error: null,
            totalCount: isHard ? null : totalCount,
            ...(isHard && {
                range: { start: 0, end: range.end - range.start },
                resetId: resetId + 1,
            }),
        });

        this.load();
    };

    cancel = () => {
        this.generation++;
        this.update({ isFetching: false });
    };

    private async load() {
        if (!this.isStarted || this.snapshot.isFetching) {
            return;
        }

        const request = this.getNextRequest();
        if (!request) {
            return;
        }

        const generation = this.generation;
        this.update({ isFetching: true, error: null });

        let result: pagedResultWithToken<T>;
        try {
            result = await this.options.fetchData(request.skip, request.take, request.continuationToken);
        } catch (error) {
            if (generation === this.generation) {
                this.update({ isFetching: false, error });
            }
            return;
        }

        if (generation !== this.generation) {
            return;
        }

        this.apply(request, result);
        this.load();
    }

    private getNextRequest(): FetchRequest | null {
        const { range, allowSkip, totalCount, loadedCount, hasMore } = this.snapshot;
        const { fetchMode, minFetchCount } = this.options;
        const end = Math.min(range.end, totalCount ?? Infinity);

        if (range.start >= end) {
            return null;
        }

        if (fetchMode === "continuationToken" && (range.start <= loadedCount || !allowSkip)) {
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

        return {
            skip: missing.start,
            take: Math.min(
                Math.max(minFetchCount, missing.end - missing.start),
                (totalCount ?? Infinity) - missing.start
            ),
            isSequential: false,
        };
    }

    private apply({ skip, take, isSequential }: FetchRequest, result: pagedResultWithToken<T>) {
        result.items.forEach((item, i) => this.items.set(skip + i, item));

        const loadedEnd = skip + result.items.length;
        const reportedTotal =
            typeof result.totalResultCount === "number" && result.totalResultCount >= 0
                ? result.totalResultCount
                : null;
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

    private update(changes: Partial<LazyRowsSnapshot>) {
        this.snapshot = { ...this.snapshot, ...changes };
        this.listeners.forEach((listener) => listener());
    }
}
