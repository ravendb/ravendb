import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { virtualTableConstants } from "../utils/virtualTableConstants";
import {
    findMissingRange,
    getFitPageSize,
    getPageSizeOptions,
    isInRange,
    isSameRange,
    RowRange,
    snapDown,
    snapUp,
} from "../utils/lazyTableUtils";
import { useMutableState } from "./useMutableState";
import { VirtualTableArea } from "./useVirtualTableArea";

export type { RowRange } from "../utils/lazyTableUtils";
export { lazyVirtualTablePageSizeOptions } from "../utils/lazyTableUtils";

export type LazyVirtualTableFetchMode = "skipTake" | "continuationToken";

export type LazyVirtualTableFetchData<T> = (
    skip: number,
    take: number,
    continuationToken?: string
) => Promise<pagedResultWithToken<T>>;

export interface LazyRow<T> {
    index: number;
    item: T;
}

export interface LazyVirtualTablePagination {
    page: number;
    totalPages: number;
    firstRowNumber: number;
    lastRowNumber: number;
    totalCount: number | null;
    pageSize: number;
    pageSizeOptions: number[];
    onPageChange: (page: number) => void;
    onPageSizeChange: (pageSize: number) => void;
    turnOff: (() => void) | null;
}

export interface LazyVirtualTableView {
    areaRef: React.RefObject<HTMLDivElement>;
    containerRef: React.MutableRefObject<HTMLDivElement>;
    heightInPx: number;
    isCompact: boolean;
    rowHeightInPx: number;
    bodyHeightInPx: number;
    firstRowIndex: number;
    isLoading: boolean;
    isEmpty: boolean;
    isDomLimitBannerVisible: boolean;
    isScrollToTopVisible: boolean;
    onTurnOnPagination: () => void;
    onScrollToTop: () => void;
    pagination: LazyVirtualTablePagination | null;
}

export interface LazyVirtualTable<T> {
    data: T[];
    rows: LazyRow<T>[];
    totalCount: number | null;
    isPaginated: boolean;
    setIsPaginated: (value: boolean) => void;
    reload: () => void;
    view: LazyVirtualTableView;
}

interface UseLazyVirtualTableProps<T> {
    area: VirtualTableArea;
    fetchData: LazyVirtualTableFetchData<T>;
    fetchMode?: LazyVirtualTableFetchMode;
    isCompact?: boolean;
    overscan?: number;
    minFetchCount?: number;
    reloadDependencies?: unknown[];
}

interface LazyTableState<T> {
    items: Map<number, T>;
    totalCount: number | null;
    continuationToken: string | undefined;
    loadedCount: number;
    hasMore: boolean;
    window: RowRange;
    page: number | null;
    isAtBottom: boolean;
    isScrolledFromTop: boolean;
    // a fetch is in flight, the loading indicator (isLoading) follows it with a delay when rows are on screen
    isFetching: boolean;
    isLoading: boolean;
}

const loadingIndicatorDelayInMs = 150;
const scrollFetchDebounceInMs = 50;
const emptyWindow: RowRange = { start: 0, end: 0 };

function createInitialState<T>(): LazyTableState<T> {
    return {
        items: new Map(),
        totalCount: null,
        continuationToken: undefined,
        loadedCount: 0,
        hasMore: true,
        window: emptyWindow,
        page: null,
        isAtBottom: false,
        isScrolledFromTop: false,
        isFetching: false,
        isLoading: false,
    };
}

export function useLazyVirtualTable<T>({
    area,
    fetchData,
    fetchMode = "skipTake",
    isCompact = false,
    overscan = 20,
    minFetchCount = 100,
    reloadDependencies = [],
}: UseLazyVirtualTableProps<T>): LazyVirtualTable<T> {
    const containerRef = useRef<HTMLDivElement>(null);
    const [selectedPageSize, setSelectedPageSize] = useState<number | null>(null);
    const [isPaginationFromBanner, setIsPaginationFromBanner] = useState(false);

    const rowHeightInPx = isCompact
        ? virtualTableConstants.compactRowHeightInPx
        : virtualTableConstants.defaultRowHeightInPx;
    const maxRowsInDom = Math.floor(virtualTableConstants.maxBodyHeightInPx / rowHeightInPx);
    // the window moves in steps, so scrolling by a single row does not re-render the table
    const windowSnapInRows = Math.max(1, Math.floor(overscan / 2));
    const isTokenMode = fetchMode === "continuationToken";

    const state = useMutableState(createInitialState<T>);
    const { ref: stateRef, set: commit, setDeferred: commitDeferred } = state;
    const { items, totalCount, hasMore, loadedCount, window, page, isFetching, isLoading } = state.value;

    const isPaginated = page !== null;
    const fitPageSize = getFitPageSize(area.heightInPx, rowHeightInPx, isCompact);
    const defaultPageSize = Math.max(1, Math.min(fitPageSize, totalCount ?? Infinity));
    const pageSize = selectedPageSize ?? defaultPageSize;
    const pageSizeRef = useRef(pageSize);
    pageSizeRef.current = pageSize;

    const fetchDataRef = useRef(fetchData);
    fetchDataRef.current = fetchData;
    const generationRef = useRef(0);
    const pendingRangesRef = useRef<RowRange[]>([]);
    const fetchTimeoutRef = useRef<ReturnType<typeof setTimeout>>(null);
    const scrollTopToRestoreRef = useRef<number>(null);

    const cancelPendingFetches = () => {
        generationRef.current++;
        pendingRangesRef.current = [];
        clearTimeout(fetchTimeoutRef.current);
        commit({ isFetching: false, isLoading: false });
    };

    const runFetch = async (
        skip: number,
        take: number,
        continuationToken: string,
        apply: (result: pagedResultWithToken<T>) => void
    ) => {
        const generation = generationRef.current;
        const pendingRange: RowRange = { start: skip, end: skip + take };
        pendingRangesRef.current = [...pendingRangesRef.current, pendingRange];
        commit({ isFetching: true });

        const indicatorDelay = hasRowsOnScreen(stateRef.current) ? loadingIndicatorDelayInMs : 0;
        const indicatorTimeout = setTimeout(() => {
            if (generation === generationRef.current) {
                commit({ isLoading: true });
            }
        }, indicatorDelay);

        let result: pagedResultWithToken<T> = null;
        try {
            result = await fetchDataRef.current(skip, take, continuationToken);
        } catch {
            // failures are reported to the user by the command layer, the next scroll retries
        }

        clearTimeout(indicatorTimeout);

        if (generation !== generationRef.current) {
            return;
        }

        pendingRangesRef.current = pendingRangesRef.current.filter((x) => x !== pendingRange);

        if (result) {
            apply(result);
        }

        const isIdle = pendingRangesRef.current.length === 0;
        commitDeferred({
            isFetching: !isIdle,
            isLoading: !isIdle && stateRef.current.isLoading && hasMissingWindowRows(stateRef.current, maxRowsInDom),
        });

        if (result) {
            refreshRef.current();
        }
    };

    const isPending = (rowIndex: number) => pendingRangesRef.current.some((x) => isInRange(x, rowIndex));

    const applySkipTakeResult = (skip: number, take: number, result: pagedResultWithToken<T>) => {
        const nextItems = new Map(stateRef.current.items);
        result.items.forEach((item, i) => nextItems.set(skip + i, item));

        const loadedEnd = skip + result.items.length;
        const isLastBatch = result.items.length < take;
        const nextTotalCount = result.totalResultCount ?? stateRef.current.totalCount ?? loadedEnd;

        commitDeferred({
            items: nextItems,
            totalCount: isLastBatch ? Math.min(nextTotalCount, loadedEnd) : nextTotalCount,
        });
    };

    const applyTokenResult = (result: pagedResultWithToken<T>) => {
        const { items, loadedCount } = stateRef.current;

        const nextItems = new Map(items);
        result.items.forEach((item, i) => nextItems.set(loadedCount + i, item));

        commitDeferred({
            items: nextItems,
            loadedCount: loadedCount + result.items.length,
            continuationToken: result.continuationToken,
            hasMore: !!result.continuationToken && result.items.length > 0,
            totalCount: result.totalResultCount ?? null,
        });
    };

    // fetches whatever the current window is still missing, pages are fetched by skip and take in both modes
    const loadWindow = () => {
        const { items, totalCount, window, page, hasMore, continuationToken, loadedCount, isFetching } =
            stateRef.current;

        if (isTokenMode && page === null) {
            if (isFetching || !hasMore || window.end <= loadedCount || loadedCount >= maxRowsInDom) {
                return;
            }

            runFetch(loadedCount, minFetchCount, continuationToken, applyTokenResult);
            return;
        }

        const lastRow = page === null ? Math.min(totalCount ?? Infinity, maxRowsInDom) : (totalCount ?? Infinity);
        const missing = findMissingRange(
            { start: window.start, end: Math.min(window.end, lastRow) },
            (i) => items.has(i) || isPending(i)
        );

        if (!missing) {
            return;
        }

        const minTake = page === null ? minFetchCount : 1;
        const take = Math.min(Math.max(minTake, missing.end - missing.start), (totalCount ?? Infinity) - missing.start);

        runFetch(missing.start, take, undefined, (result) => applySkipTakeResult(missing.start, take, result));
    };

    const updateWindowFromScroll = () => {
        const element = containerRef.current;
        if (!element) {
            commit({ window: { start: 0, end: minFetchCount } });
            return;
        }

        const firstVisibleRow = Math.floor(element.scrollTop / rowHeightInPx);
        const lastVisibleRow = Math.ceil((element.scrollTop + element.clientHeight) / rowHeightInPx);
        const nextWindow: RowRange = {
            start: Math.max(0, snapDown(firstVisibleRow - overscan, windowSnapInRows)),
            end: snapUp(lastVisibleRow + overscan, windowSnapInRows),
        };

        commitDeferred({
            window: isSameRange(nextWindow, stateRef.current.window) ? stateRef.current.window : nextWindow,
            isAtBottom: element.scrollTop + element.clientHeight >= element.scrollHeight - 1,
            isScrolledFromTop: element.scrollTop > 0,
        });
    };

    // brings the window up to date with the scroll position and starts the fetches it needs
    const refresh = () => {
        if (stateRef.current.page === null) {
            updateWindowFromScroll();
        }

        clearTimeout(fetchTimeoutRef.current);

        if (pendingRangesRef.current.length === 0) {
            loadWindow();
        } else {
            fetchTimeoutRef.current = setTimeout(() => refreshRef.current(), scrollFetchDebounceInMs);
        }
    };

    const refreshRef = useRef(refresh);
    refreshRef.current = refresh;

    const goToPage = (nextPage: number) => {
        cancelPendingFetches();
        scrollTopToRestoreRef.current = 0;

        const start = (nextPage - 1) * pageSizeRef.current;
        commit({ page: nextPage, window: { start, end: start + pageSizeRef.current } });

        loadWindow();
    };

    const goToPageRef = useRef(goToPage);
    goToPageRef.current = goToPage;

    // soft reset keeps the scroll position, total count and current page
    const reset = (isHard: boolean) => {
        cancelPendingFetches();

        const { totalCount, page } = stateRef.current;
        const nextPage = page === null ? null : isHard ? 1 : page;

        commit({
            ...createInitialState<T>(),
            totalCount: isHard ? null : totalCount,
            page: nextPage,
            window: nextPage === null ? emptyWindow : pageWindow(nextPage, pageSizeRef.current),
        });

        if (isHard && containerRef.current) {
            containerRef.current.scrollTop = 0;
        }

        refresh();
    };

    const resetRef = useRef(reset);
    resetRef.current = reset;

    const reload = useCallback(() => resetRef.current(false), []);

    useLayoutEffect(() => {
        resetRef.current(true);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, reloadDependencies);

    useEffect(() => {
        const element = containerRef.current;
        if (!element) {
            return;
        }

        const handleScroll = () => refreshRef.current();

        element.addEventListener("scroll", handleScroll, { passive: true });

        const resizeObserver = new ResizeObserver(handleScroll);
        resizeObserver.observe(element);

        return () => {
            element.removeEventListener("scroll", handleScroll);
            resizeObserver.disconnect();
            clearTimeout(fetchTimeoutRef.current);
        };
    }, []);

    // the page size changed while paginated (the table was resized or another size was picked),
    // so the page is reloaded with the new size keeping its first row on screen
    useEffect(() => {
        if (stateRef.current.page !== null) {
            goToPageRef.current(Math.floor(stateRef.current.window.start / pageSize) + 1);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [pageSize]);

    const scrollableRowCount = Math.min(isTokenMode ? loadedCount : (totalCount ?? 0), maxRowsInDom);
    const isDomLimitReached = isTokenMode ? loadedCount >= maxRowsInDom && hasMore : totalCount > maxRowsInDom;

    const rows = useMemo(() => {
        const lastRow = isPaginated ? (totalCount ?? window.end) : scrollableRowCount;
        const result: LazyRow<T>[] = [];

        for (let i = window.start; i < Math.min(window.end, lastRow); i++) {
            const item = items.get(i);
            if (item !== undefined) {
                result.push({ index: i, item });
            }
        }

        return result;
    }, [isPaginated, totalCount, window, items, scrollableRowCount]);

    const data = useMemo(() => rows.map((x) => x.item), [rows]);

    const pageRowCount = totalCount === null ? rows.length : Math.max(0, Math.min(pageSize, totalCount - window.start));
    const bodyHeightInPx = (isPaginated ? pageRowCount : scrollableRowCount) * rowHeightInPx;

    useLayoutEffect(() => {
        if (containerRef.current && scrollTopToRestoreRef.current !== null) {
            containerRef.current.scrollTop = scrollTopToRestoreRef.current;
            scrollTopToRestoreRef.current = null;
        }

        refreshRef.current();
    }, [bodyHeightInPx, isPaginated]);

    const turnOnPagination = (isFromBanner: boolean) => {
        const firstVisibleRow = Math.floor((containerRef.current?.scrollTop ?? 0) / rowHeightInPx);
        setIsPaginationFromBanner(isFromBanner);
        goToPage(Math.floor(firstVisibleRow / pageSize) + 1);
    };

    const turnOffPagination = () => {
        cancelPendingFetches();

        const lastReachableRow = Math.max(0, scrollableRowCount - 1);
        scrollTopToRestoreRef.current = Math.min(window.start, lastReachableRow) * rowHeightInPx;

        setIsPaginationFromBanner(false);
        commit({ page: null, window: emptyWindow });
    };

    const pageSizeOptions = useMemo(
        () => getPageSizeOptions(defaultPageSize, totalCount),
        [defaultPageSize, totalCount]
    );

    const isEmpty =
        !isFetching &&
        (isPaginated ? rows.length === 0 : isTokenMode ? loadedCount === 0 && !hasMore : totalCount === 0);

    return {
        data,
        rows,
        totalCount,
        isPaginated,
        setIsPaginated: (value) => {
            if (value !== isPaginated) {
                value ? turnOnPagination(false) : turnOffPagination();
            }
        },
        reload,
        view: {
            areaRef: area.ref,
            containerRef,
            heightInPx: area.heightInPx,
            isCompact,
            rowHeightInPx,
            bodyHeightInPx,
            firstRowIndex: isPaginated ? window.start : 0,
            isLoading,
            isEmpty,
            isDomLimitBannerVisible: isDomLimitReached && state.value.isAtBottom && !isPaginated,
            isScrollToTopVisible: state.value.isScrolledFromTop && !isPaginated,
            onTurnOnPagination: () => turnOnPagination(true),
            onScrollToTop: () => containerRef.current?.scrollTo({ top: 0, behavior: "smooth" }),
            pagination: isPaginated
                ? {
                      page,
                      totalPages: totalCount
                          ? Math.ceil(totalCount / pageSize)
                          : page + (rows.length === pageSize ? 1 : 0),
                      firstRowNumber: pageRowCount > 0 ? window.start + 1 : 0,
                      lastRowNumber: window.start + pageRowCount,
                      totalCount,
                      pageSize,
                      pageSizeOptions,
                      onPageChange: goToPage,
                      onPageSizeChange: (size) => setSelectedPageSize(size === defaultPageSize ? null : size),
                      turnOff: isPaginationFromBanner ? turnOffPagination : null,
                  }
                : null,
        },
    };
}

function pageWindow(page: number, pageSize: number): RowRange {
    return { start: (page - 1) * pageSize, end: page * pageSize };
}

function hasMissingWindowRows<T>({ items, totalCount, window }: LazyTableState<T>, maxRowsInDom: number) {
    const end = Math.min(window.end, totalCount ?? Infinity, maxRowsInDom);

    return findMissingRange({ start: window.start, end }, (i) => items.has(i)) !== null;
}

function hasRowsOnScreen<T>({ items, window }: LazyTableState<T>) {
    for (let i = window.start; i < window.end; i++) {
        if (items.has(i)) {
            return true;
        }
    }

    return false;
}
