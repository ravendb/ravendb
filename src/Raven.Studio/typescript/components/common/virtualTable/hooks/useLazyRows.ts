import { useLayoutEffect, useMemo, useState, useSyncExternalStore } from "react";
import {
    LazyFetchData,
    LazyFetchMode,
    LazyRow,
    LazyRowsLoader,
} from "components/common/virtualTable/utils/LazyRowsLoader";
import { RowRange } from "components/common/virtualTable/utils/lazyTableUtils";

interface UseLazyRowsProps<T, TResult extends pagedResultWithToken<T>> {
    fetchData: LazyFetchData<T, TResult>;
    fetchMode?: LazyFetchMode;
    minFetchCount?: number;
    onResult?: (result: TResult) => void;
    onReset?: () => void;
    reloadDependencies?: unknown[];
}

export interface LazyRows<T> {
    data: T[];
    rows: LazyRow<T>[];
    totalCount: number | null;
    loadedCount: number;
    hasMore: boolean;
    fetchMode: LazyFetchMode;
    isFetching: boolean;
    error: unknown;
    resetId: number;
    getItem: (rowIndex: number) => T | undefined;
    setRange: (range: RowRange) => void;
    reload: () => void;
    retry: () => void;
}

export function useLazyRows<T, TResult extends pagedResultWithToken<T> = pagedResultWithToken<T>>({
    fetchData,
    fetchMode = "skipTake",
    minFetchCount = 100,
    onResult,
    onReset,
    reloadDependencies = [],
}: UseLazyRowsProps<T, TResult>): LazyRows<T> {
    const options = { fetchData, fetchMode, minFetchCount, onResult, onReset };
    const [loader] = useState(() => new LazyRowsLoader<T, TResult>(options));

    useLayoutEffect(() => {
        loader.setOptions(options);
    });

    const snapshot = useSyncExternalStore(loader.subscribe, loader.getSnapshot);

    // Starts the loading over before the stale rows are painted, the results of the previous loading are dropped
    useLayoutEffect(() => {
        loader.reset(true);
        return () => loader.cancel();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fetchMode, minFetchCount, ...reloadDependencies]);

    const rows = useMemo(
        () => loader.getRows(),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [snapshot.version, snapshot.range, snapshot.loadedCount, snapshot.totalCount]
    );
    const data = useMemo(() => rows.map((x) => x.item), [rows]);

    return {
        data,
        rows,
        totalCount: snapshot.totalCount,
        loadedCount: snapshot.loadedCount,
        hasMore: snapshot.hasMore,
        fetchMode,
        isFetching: snapshot.isFetching,
        error: snapshot.error,
        resetId: snapshot.resetId,
        getItem: loader.getItem,
        setRange: loader.setRange,
        reload: () => loader.reset(false),
        retry: loader.retry,
    };
}
