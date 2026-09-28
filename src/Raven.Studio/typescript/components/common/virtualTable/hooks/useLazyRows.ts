import { useCallback, useEffect, useLayoutEffect, useMemo, useState, useSyncExternalStore } from "react";
import {
    LazyFetchData,
    LazyFetchMode,
    LazyRow,
    LazyRowsLoader,
    SetRangeOptions,
} from "components/common/virtualTable/utils/LazyRowsLoader";
import { RowRange } from "components/common/virtualTable/utils/lazyTableUtils";

interface UseLazyRowsProps<T, TResult extends pagedResultWithToken<T>> {
    fetchData: LazyFetchData<T, TResult>;
    fetchMode?: LazyFetchMode;
    minFetchCount?: number;
    // called only with the results that are applied, the ones fetched before a reset are dropped
    onResult?: (result: TResult) => void;
    // the rows are cleared and fetched again from the first one whenever any of these change
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
    resetId: number;
    getItem: (rowIndex: number) => T | undefined;
    setRange: (range: RowRange, options?: SetRangeOptions) => void;
    // fetches the rows again keeping the total count and the position
    reload: () => void;
}

export function useLazyRows<T, TResult extends pagedResultWithToken<T> = pagedResultWithToken<T>>({
    fetchData,
    fetchMode = "skipTake",
    minFetchCount = 100,
    onResult,
    reloadDependencies = [],
}: UseLazyRowsProps<T, TResult>): LazyRows<T> {
    const [loader] = useState(() => new LazyRowsLoader<T, TResult>({ fetchData, fetchMode, minFetchCount, onResult }));
    loader.setOptions({ fetchData, fetchMode, minFetchCount, onResult });

    const snapshot = useSyncExternalStore(loader.subscribe, loader.getSnapshot);

    useLayoutEffect(() => {
        loader.reset(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [fetchMode, minFetchCount, ...reloadDependencies]);

    useEffect(() => () => loader.cancel(), [loader]);

    const rows = useMemo(
        () => loader.getRows(),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        [loader, snapshot.version, snapshot.range, snapshot.allowSkip, snapshot.loadedCount, snapshot.totalCount]
    );
    const data = useMemo(() => rows.map((x) => x.item), [rows]);

    const reload = useCallback(() => loader.reset(false), [loader]);

    return {
        data,
        rows,
        totalCount: snapshot.totalCount,
        loadedCount: snapshot.loadedCount,
        hasMore: snapshot.hasMore,
        fetchMode,
        isFetching: snapshot.isFetching,
        resetId: snapshot.resetId,
        getItem: loader.getItem,
        setRange: loader.setRange,
        reload,
    };
}
