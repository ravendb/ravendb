import { useCallback, useLayoutEffect, useRef, useState } from "react";
import useTimeout from "components/hooks/useTimeout";
import { useResizeObserver } from "components/hooks/useResizeObserver";
import { LazyRows } from "components/common/virtualTable/hooks/useLazyRows";
import { LazyVirtualTablePagination } from "components/common/virtualTable/partials/LazyVirtualTablePaginationBar";
import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { isSameRange, RowRange } from "components/common/virtualTable/utils/lazyTableUtils";

interface UseLazyTableViewportProps<T> {
    lazyRows: LazyRows<T>;
    isPaginated: boolean;
    setIsPaginated: (isPaginated: boolean) => void;
    fixedHeightInPx?: number;
}

const {
    defaultRowHeightInPx: rowHeightInPx,
    headerHeightInPx,
    maxBodyHeightInPx,
    defaultTableHeightInPx,
} = virtualTableConstants;
const maxRowsInDom = Math.floor(maxBodyHeightInPx / rowHeightInPx);
const overscanInRows = 20;
const windowStepInRows = 10;
const pageSizeOptions = [25, 50, 100];
const loadingIndicatorDelayInMs = 150;

export function useLazyTableViewport<T>({
    lazyRows,
    isPaginated,
    setIsPaginated,
    fixedHeightInPx,
}: UseLazyTableViewportProps<T>) {
    const { rows, totalCount, loadedCount, hasMore, fetchMode, isFetching, resetId, setRange } = lazyRows;

    const areaRef = useRef<HTMLDivElement>(null);
    const containerRef = useRef<HTMLDivElement>(null);
    const lastScrollTopRef = useRef(0);

    const measuredArea = useResizeObserver({ ref: areaRef });
    const heightInPx = fixedHeightInPx ?? measuredArea.height ?? defaultTableHeightInPx;

    const [scrollRange, setScrollRange] = useState(() => getScrollRange(0, 0));
    const [isAtBottom, setIsAtBottom] = useState(false);
    const [pageFirstRowIndex, setPageFirstRowIndex] = useState<number>(null);
    const [selectedPageSize, setSelectedPageSize] = useState<number>(null);
    const [isPaginationFromBanner, setIsPaginationFromBanner] = useState(false);
    const [isFetchingLong, setIsFetchingLong] = useState(false);

    const isSkipTake = fetchMode === "skipTake";
    const defaultPageSize = Math.min(getFitPageSize(heightInPx), totalCount || Infinity);
    const pageSize = selectedPageSize ?? defaultPageSize;
    const page = Math.floor((pageFirstRowIndex ?? 0) / pageSize) + 1;
    const pageStart = (page - 1) * pageSize;
    const pageRowCount = totalCount === null ? rows.length : Math.max(0, Math.min(pageSize, totalCount - pageStart));
    const scrollableRowCount = Math.min(isSkipTake ? (totalCount ?? 0) : loadedCount, maxRowsInDom);
    const bodyHeightInPx = (isPaginated ? pageRowCount : scrollableRowCount) * rowHeightInPx;
    const range = isPaginated ? { start: pageStart, end: pageStart + pageSize } : scrollRange;

    const updateScrollState = useCallback(() => {
        if (isPaginated) {
            return;
        }

        const { scrollTop, clientHeight, scrollHeight } = containerRef.current;
        lastScrollTopRef.current = scrollTop;

        const nextRange = getScrollRange(scrollTop, clientHeight);
        setScrollRange((prev) => (isSameRange(prev, nextRange) ? prev : nextRange));
        setIsAtBottom(scrollTop + clientHeight >= scrollHeight - 1);
    }, [isPaginated]);

    // Keeps the same rows on the screen when switching between the scrolled and the paginated view.
    // The last scroll position is used, because the browser clamps the current one when the body shrinks.
    useLayoutEffect(() => {
        if (isPaginated) {
            setPageFirstRowIndex(Math.floor(lastScrollTopRef.current / rowHeightInPx));
        } else if (pageFirstRowIndex !== null) {
            containerRef.current.scrollTop = Math.min(pageStart, scrollableRowCount - 1) * rowHeightInPx;
            setPageFirstRowIndex(null);
            setIsPaginationFromBanner(false);
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isPaginated]);

    // Reloaded rows are shown from the first one
    useLayoutEffect(() => {
        containerRef.current.scrollTop = 0;
        setPageFirstRowIndex((prev) => (prev === null ? null : 0));
    }, [resetId]);

    // Resizing the container or its body changes the visible rows without a scroll event
    useLayoutEffect(() => {
        updateScrollState();
    }, [heightInPx, bodyHeightInPx, updateScrollState]);

    // Fetches the rows of the visible range, the page is fetched once it is picked from the scroll position
    useLayoutEffect(() => {
        if (!isPaginated || pageFirstRowIndex !== null) {
            setRange(range, { allowSkip: isPaginated });
        }
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [range.start, range.end, isPaginated, pageFirstRowIndex, setRange]);

    useTimeout(() => setIsFetchingLong(true), isFetching ? loadingIndicatorDelayInMs : null);
    if (!isFetching && isFetchingLong) {
        setIsFetchingLong(false);
    }

    const scrollToTop = () => {
        containerRef.current.scrollTop = 0;
    };

    const pagination: LazyVirtualTablePagination = isPaginated
        ? {
              page,
              totalPages: totalCount ? Math.ceil(totalCount / pageSize) : page + (rows.length === pageSize ? 1 : 0),
              firstRowNumber: pageRowCount > 0 ? pageStart + 1 : 0,
              lastRowNumber: pageStart + pageRowCount,
              totalCount,
              pageSize,
              pageSizeOptions: [
                  defaultPageSize,
                  ...pageSizeOptions.filter((x) => x > defaultPageSize && (totalCount === null || x < totalCount)),
              ],
              onPageChange: (nextPage) => {
                  scrollToTop();
                  setPageFirstRowIndex((nextPage - 1) * pageSize);
              },
              onPageSizeChange: (nextPageSize) => {
                  scrollToTop();
                  setSelectedPageSize(nextPageSize === defaultPageSize ? null : nextPageSize);
              },
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
        bodyHeightInPx,
        firstRowIndex: isPaginated ? pageStart : 0,
        isLoading: isFetching && (rows.length === 0 || isFetchingLong),
        isEmpty,
        isDomLimitBannerVisible: isDomLimitReached && isAtBottom && !isPaginated,
        turnOnPagination: () => {
            setIsPaginationFromBanner(true);
            setIsPaginated(true);
        },
        pagination,
        onScroll: updateScrollState,
    };
}

function getScrollRange(scrollTop: number, clientHeight: number): RowRange {
    const firstVisibleRow = Math.floor(scrollTop / rowHeightInPx);
    const lastVisibleRow = Math.ceil((scrollTop + clientHeight) / rowHeightInPx);

    return {
        start: Math.max(0, snapDown(firstVisibleRow - overscanInRows)),
        end: Math.min(snapUp(lastVisibleRow + overscanInRows), maxRowsInDom),
    };
}

function snapDown(rowIndex: number) {
    return Math.floor(rowIndex / windowStepInRows) * windowStepInRows;
}

function snapUp(rowIndex: number) {
    return Math.ceil(rowIndex / windowStepInRows) * windowStepInRows;
}

function getFitPageSize(heightInPx: number) {
    const rowsHeightInPx = virtualTableUtils.getTableContainerHeightInPx(heightInPx) - headerHeightInPx;
    return Math.max(1, Math.floor(rowsHeightInPx / rowHeightInPx));
}
