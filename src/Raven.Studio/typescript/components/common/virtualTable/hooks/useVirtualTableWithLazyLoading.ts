import { useEffect, useMemo, useRef, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { virtualTableConstants } from "../utils/virtualTableConstants";

// Use it along with VirtualTableWithLazyLoading component

type FetchData<T extends pagedResult<unknown>> = (skip: number, take: number) => Promise<T>;

interface UseVirtualTableWithLazyLoadingProps<T extends pagedResult<unknown>> {
    fetchData: FetchData<T>;
    overscan?: number;
    tableHeightInPx?: number;
}

export function useVirtualTableWithLazyLoading<T extends pagedResult<unknown>>({
    fetchData,
    overscan = 50,
    tableHeightInPx = window.innerHeight,
}: UseVirtualTableWithLazyLoadingProps<T>) {
    const tableContainerRef = useRef<HTMLDivElement>(null);

    const [dataPreview, setDataPreview] = useState<T["items"]>([]);
    const [totalResultCount, setTotalResultCount] = useState<number>(0);
    const [isOnBottom, setIsOnBottom] = useState(false);
    const [lastSkipAfterLoad, setLastSkipAfterLoad] = useState(0);

    const initialItemsCount = Math.ceil(tableHeightInPx / defaultRowHeightInPx) + overscan;
    const scalar = useMemo(() => getScalar(totalResultCount), [totalResultCount]);

    const dataStartIndex = useRef(0);
    const dataEndIndex = useRef(initialItemsCount);

    const bodyHeightInPx = Math.floor((totalResultCount * defaultRowHeightInPx) / scalar);

    const asyncLoadInitialData = useAsync(async () => {
        const result = await fetchData(0, initialItemsCount);
        setTotalResultCount(result.totalResultCount);
        setDataPreview(result.items);
    }, []);

    const asyncLoadData = useAsyncCallback(async (skip: number, take: number) => {
        const result = await fetchData(skip, take);
        setTotalResultCount(result.totalResultCount);
        setDataPreview(result.items);

        setLastSkipAfterLoad(skip);
        setIsOnBottom(skip + take === totalResultCount);
    });

    // Handle scroll
    useEffect(() => {
        if (!tableContainerRef.current) {
            return;
        }

        const handleScroll = (e: Event) => {
            const target = e.target as HTMLDivElement;

            const scaledScrollTop = target.scrollTop * scalar;
            const startIndex = Math.floor(scaledScrollTop / defaultRowHeightInPx);
            const visibleElementsCount = Math.floor((target.clientHeight - headerHeightInPx) / defaultRowHeightInPx);
            const isOnBottom = target.scrollTop + target.clientHeight >= bodyHeightInPx;

            let safeStartIndex = dataStartIndex.current;
            let safeEndIndex = dataEndIndex.current;

            const halfOverscan = overscan / 2;

            if (safeStartIndex > halfOverscan) {
                safeStartIndex += halfOverscan;
            }
            if (totalResultCount - safeEndIndex > halfOverscan) {
                safeEndIndex -= halfOverscan;
            }

            // the field over which you can scroll without fetching new data
            if (startIndex >= safeStartIndex && startIndex + visibleElementsCount <= safeEndIndex) {
                return;
            }

            let skip = startIndex - overscan;
            const take = visibleElementsCount + overscan * 2;

            if (isOnBottom || skip + take > totalResultCount) {
                skip = totalResultCount - take;
            }

            if (skip < 0) {
                skip = 0;
            }

            dataStartIndex.current = skip;
            dataEndIndex.current = skip + take;
            asyncLoadData.execute(skip, take);
        };

        const current = tableContainerRef.current;
        current.addEventListener("scroll", handleScroll);

        return () => {
            current.removeEventListener("scroll", handleScroll);
        };
    }, [asyncLoadData, bodyHeightInPx, overscan, scalar, totalResultCount]);

    const getRowPositionY = (index: number) => {
        if (isOnBottom) {
            // last element is always at the end of the table
            return bodyHeightInPx - (dataPreview.length - index) * defaultRowHeightInPx;
        }

        return (lastSkipAfterLoad * defaultRowHeightInPx) / scalar + index * defaultRowHeightInPx;
    };

    return {
        dataPreview,
        componentProps: {
            tableContainerRef,
            isLoading: asyncLoadInitialData.loading || asyncLoadData.loading,
            bodyHeightInPx,
            getRowPositionY,
        },
    };
}

const { defaultRowHeightInPx, headerHeightInPx } = virtualTableConstants;

function getScalar(allItemsCount: number) {
    // Browsers have limits on element height and transform value. With this maximum height, no problems arise.
    const safeHeightInPx = 5_000_000;
    const realHeightInPx = allItemsCount * defaultRowHeightInPx;

    if (realHeightInPx < safeHeightInPx) {
        return 1;
    }

    return realHeightInPx / safeHeightInPx;
}
