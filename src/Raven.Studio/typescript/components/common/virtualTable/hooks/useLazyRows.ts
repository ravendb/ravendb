import { useEffect, useLayoutEffect, useMemo, useState, useSyncExternalStore } from "react";
import {
    LazyFetchData,
    LazyFetchMode,
    LazyRow,
    LazyRowsLoader,
    SetRangeOptions,
} from "components/common/virtualTable/utils/LazyRowsLoader";
import { RowRange } from "components/common/virtualTable/utils/lazyTableUtils";

interface UseLazyRowsProps<T> {
    fetchData: LazyFetchData<T>;
    fetchMode?: LazyFetchMode;
    minFetchCount?: number;
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
    error: unknown;
    resetId: number;
    getItem: (rowIndex: number) => T | undefined;
    setRange: (range: RowRange, options?: SetRangeOptions) => void;
    // fetches the rows again keeping the total count and the position
    reload: () => void;
    // clears everything and goes back to the first row
    reset: () => void;
}

export function useLazyRows<T>({
    fetchData,
    fetchMode = "skipTake",
    minFetchCount = 100,
    reloadDependencies = [],
}: UseLazyRowsProps<T>): LazyRows<T> {
    const [loader] = useState(() => new LazyRowsLoader<T>({ fetchData, fetchMode, minFetchCount }));
    loader.setOptions({ fetchData, fetchMode, minFetchCount });

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

    const { reload, reset } = useMemo(
        () => ({ reload: () => loader.reset(false), reset: () => loader.reset(true) }),
        [loader]
    );

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
        reload,
        reset,
    };
}
