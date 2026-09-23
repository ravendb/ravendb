import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from "react";
import { useResizeObserver } from "components/hooks/useResizeObserver";
import { LazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { LazyVirtualTablePagination } from "components/common/virtualTable/partials/LazyVirtualTablePaginationBar";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import {
    getFitPageSize,
    getPageSizeOptions,
    isSameRange,
    RowRange,
    snapDown,
    snapUp,
} from "components/common/virtualTable/utils/lazyTableUtils";

interface UseLazyTableViewportProps<T> {
    lazyRows: LazyRows<T>;
    isPaginated: boolean;
    setIsPaginated: (isPaginated: boolean) => void;
    fixedHeightInPx?: number;
    isCompact: boolean;
    overscan: number;
}

const loadingIndicatorDelayInMs = 150;

export function useLazyTableViewport<T>({
    lazyRows,
    isPaginated,
    setIsPaginated,
    fixedHeightInPx,
    isCompact,
    overscan,
}: UseLazyTableViewportProps<T>) {
    const { rows, totalCount, loadedCount, hasMore, fetchMode, isFetching, resetId, setRange } = lazyRows;

    const areaRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    // the scroll position before pagination shrank the body, the browser may have clamped the live one already
    const lastScrollTopRef = useRef(0);
    const scrollTopToRestoreRef = useRef<number>(null);
    const lastResetIdRef = useRef(resetId);

    const measuredArea = useResizeObserver({ ref: areaRef });
    const heightInPx = fixedHeightInPx ?? measuredArea.height ?? virtualTableConstants.defaultTableHeightInPx;

    const rowHeightInPx = isCompact
        ? virtualTableConstants.compactRowHeightInPx
        : virtualTableConstants.defaultRowHeightInPx;
    const maxRowsInDom = Math.floor(virtualTableConstants.maxBodyHeightInPx / rowHeightInPx);
    // the window moves in steps, so scrolling by a single row does not re-render the table
    const windowSnapInRows = Math.max(1, Math.floor(overscan / 2));

    const getScrollRange = useCallback(
        (scrollTop: number, clientHeight: number): RowRange => {
            const firstVisibleRow = Math.floor(scrollTop / rowHeightInPx);
            const lastVisibleRow = Math.ceil((scrollTop + clientHeight) / rowHeightInPx);

            return {
                start: Math.max(0, snapDown(firstVisibleRow - overscan, windowSnapInRows)),
                end: snapUp(lastVisibleRow + overscan, windowSnapInRows),
            };
        },
        [rowHeightInPx, overscan, windowSnapInRows]
    );

    const [scrollRange, setScrollRange] = useState(() => getScrollRange(0, 0));
    const [isAtBottom, setIsAtBottom] = useState(false);
    const [isScrolledFromTop, setIsScrolledFromTop] = useState(false);
    // null until a page is chosen, the first row (not the page number) is kept so a page size change keeps it on the page
    const [pageFirstRowIndex, setPageFirstRowIndex] = useState<number>(null);
    const [selectedPageSize, setSelectedPageSize] = useState<number>(null);
    const [isPaginationFromBanner, setIsPaginationFromBanner] = useState(false);
    const [isLoading, setIsLoading] = useState(false);

    const isSkipTake = fetchMode === "skipTake";

    const fitPageSize = getFitPageSize(heightInPx, rowHeightInPx, isCompact);
    const defaultPageSize = Math.max(1, Math.min(fitPageSize, totalCount ?? Infinity));
    const pageSize = selectedPageSize ?? defaultPageSize;
    const page = pageFirstRowIndex === null ? 1 : Math.floor(pageFirstRowIndex / pageSize) + 1;
    const pageStart = (page - 1) * pageSize;

    const scrollableRowCount = Math.min(isSkipTake ? (totalCount ?? 0) : loadedCount, maxRowsInDom);
    const pageRowCount = totalCount === null ? rows.length : Math.max(0, Math.min(pageSize, totalCount - pageStart));
    const bodyHeightInPx = (isPaginated ? pageRowCount : scrollableRowCount) * rowHeightInPx;

    const range: RowRange = isPaginated
        ? { start: pageStart, end: pageStart + pageSize }
        : { start: scrollRange.start, end: Math.min(scrollRange.end, maxRowsInDom) };

    const updateScrollState = useCallback(() => {
        const element = containerRef.current;
        if (!element || isPaginated) {
            return;
        }

        lastScrollTopRef.current = element.scrollTop;

        const nextRange = getScrollRange(element.scrollTop, element.clientHeight);
        setScrollRange((prev) => (isSameRange(prev, nextRange) ? prev : nextRange));
        setIsAtBottom(element.scrollTop + element.clientHeight >= element.scrollHeight - 1);
        setIsScrolledFromTop(element.scrollTop > 0);
    }, [isPaginated, getScrollRange]);

    useEffect(() => {
        const element = containerRef.current;
        if (!element) {
            return;
        }

        element.addEventListener("scroll", updateScrollState, { passive: true });

        const resizeObserver = new ResizeObserver(updateScrollState);
        resizeObserver.observe(element);

        return () => {
            element.removeEventListener("scroll", updateScrollState);
            resizeObserver.disconnect();
        };
    }, [updateScrollState]);

    useLayoutEffect(() => {
        if (isPaginated) {
            if (pageFirstRowIndex === null) {
                setPageFirstRowIndex(Math.floor(lastScrollTopRef.current / rowHeightInPx));
            }
            return;
        }

        if (pageFirstRowIndex !== null) {
            scrollTopToRestoreRef.current = Math.min(pageStart, Math.max(0, scrollableRowCount - 1)) * rowHeightInPx;
            setPageFirstRowIndex(null);
            setIsPaginationFromBanner(false);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isPaginated]);

    useLayoutEffect(() => {
        if (containerRef.current && scrollTopToRestoreRef.current !== null) {
            containerRef.current.scrollTop = scrollTopToRestoreRef.current;
            scrollTopToRestoreRef.current = null;
        }

        updateScrollState();
    }, [bodyHeightInPx, isPaginated, updateScrollState]);

    useLayoutEffect(() => {
        if (lastResetIdRef.current === resetId) {
            return;
        }
        lastResetIdRef.current = resetId;

        if (containerRef.current) {
            containerRef.current.scrollTop = 0;
        }

        updateScrollState();

        if (isPaginated) {
            setPageFirstRowIndex(0);
        }
    }, [resetId, isPaginated, updateScrollState]);

    useLayoutEffect(() => {
        if (isPaginated && pageFirstRowIndex === null) {
            return;
        }

        setRange(range, { allowSkip: isPaginated });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [range.start, range.end, isPaginated, pageFirstRowIndex, setRange]);

    const hasRowsOnScreen = rows.length > 0;

    useEffect(() => {
        if (!isFetching) {
            setIsLoading(false);
            return;
        }

        if (!hasRowsOnScreen) {
            setIsLoading(true);
            return;
        }

        const timeout = setTimeout(() => setIsLoading(true), loadingIndicatorDelayInMs);
        return () => clearTimeout(timeout);
    }, [isFetching, hasRowsOnScreen]);

    const scrollContainerToTop = () => {
        if (containerRef.current) {
            containerRef.current.scrollTop = 0;
        }
    };

    const goToPage = (nextPage: number) => {
        scrollContainerToTop();
        setPageFirstRowIndex((nextPage - 1) * pageSize);
    };

    const changePageSize = (nextPageSize: number) => {
        scrollContainerToTop();
        setSelectedPageSize(nextPageSize === defaultPageSize ? null : nextPageSize);
    };

    const pageSizeOptions = useMemo(
        () => getPageSizeOptions(defaultPageSize, totalCount),
        [defaultPageSize, totalCount]
    );

    const pagination: LazyVirtualTablePagination = isPaginated
        ? {
              page,
              totalPages: totalCount ? Math.ceil(totalCount / pageSize) : page + (rows.length === pageSize ? 1 : 0),
              firstRowNumber: pageRowCount > 0 ? pageStart + 1 : 0,
              lastRowNumber: pageStart + pageRowCount,
              totalCount,
              pageSize,
              pageSizeOptions,
              onPageChange: goToPage,
              onPageSizeChange: changePageSize,
              turnOff: isPaginationFromBanner ? () => setIsPaginated(false) : null,
          }
        : null;

    const isDomLimitReached = isSkipTake ? totalCount > maxRowsInDom : loadedCount >= maxRowsInDom && hasMore;

    const isEmpty =
        !isFetching &&
        (isPaginated ? rows.length === 0 : isSkipTake ? totalCount === 0 : loadedCount === 0 && !hasMore);

    return {
        areaRef,
        containerRef,
        heightInPx,
        rowHeightInPx,
        bodyHeightInPx,
        firstRowIndex: isPaginated ? pageStart : 0,
        isLoading,
        isEmpty,
        isDomLimitBannerVisible: isDomLimitReached && isAtBottom && !isPaginated,
        isScrollToTopVisible: isScrolledFromTop && !isPaginated,
        turnOnPagination: () => {
            setIsPaginationFromBanner(true);
            setIsPaginated(true);
        },
        scrollToTop: () => containerRef.current?.scrollTo({ top: 0, behavior: "smooth" }),
        pagination,
    };
}
