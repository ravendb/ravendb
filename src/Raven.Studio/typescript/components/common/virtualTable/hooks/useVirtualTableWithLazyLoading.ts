import { useEffect, useMemo, useRef, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { virtualTableConstants } from "../utils/virtualTableConstants";

interface UseVirtualTableWithLazyLoadingProps<T> {
    fetchData: (count: number, elementNumber: number) => Promise<pagedResult<T>>;
    overscan?: number;
    debounceInMs?: number;
}

export function useVirtualTableWithLazyLoading<T>({
    fetchData,
    overscan = 50,
    debounceInMs = 200,
}: UseVirtualTableWithLazyLoadingProps<T>) {
    const tableContainerRef = useRef<HTMLDivElement>(null);

    const initialItemsCount = Math.ceil(window.innerHeight / defaultRowHeightInPx) + overscan;

    const [dataArray, setDataArray] = useState<any[]>([]);
    const [totalResultCount, setTotalResultCount] = useState<number>(0);

    const scalar = useMemo(() => getScalar(totalResultCount), [totalResultCount]);

    const [scrollTopAfterLoad, setScrollTopAfterLoad] = useState(0);
    const [visibleElementsCount, setVisibleElementsCount] = useState(0);
    const [isOnBottom, setIsOnBottom] = useState(false);

    const [lastSkip, setLastSkip] = useState<number>(0);
    const [lastTake, setLastTake] = useState<number>(initialItemsCount);

    const asyncLoadInitialData = useAsync(async () => {
        const result = await fetchData(0, initialItemsCount);

        setDataArray(result.items);
        setTotalResultCount(result.totalResultCount);
    }, []);

    const asyncLoadData = useAsyncCallback(async (skip: number, take: number) => {
        if (skip + take > totalResultCount) {
            skip = totalResultCount - take;
        }

        if (skip < 0) {
            skip = 0;
        }

        if (skip === lastSkip && take === lastTake) {
            return;
        }

        setLastSkip(skip);
        setLastTake(take);

        const result = await fetchData(skip, take);
        setDataArray(result.items);

        setTimeout(() => {
            setScrollTopAfterLoad(tableContainerRef.current.scrollTop);
        }, debounceInMs);
    });

    const debouncedLoadData = useMemo(
        () =>
            _.debounce(async (skip: number, take: number) => {
                await asyncLoadData.execute(skip, take);
            }, debounceInMs),
        // eslint-disable-next-line react-hooks/exhaustive-deps
        []
    );

    useEffect(() => {
        if (!tableContainerRef.current) {
            return;
        }

        const handleScroll = (e: Event) => {
            const target = e.target as HTMLDivElement;

            const scrollTop = target.scrollTop;
            const scaledScrollTop = scrollTop * scalar;

            const startElementIndex = Math.floor(scaledScrollTop / defaultRowHeightInPx);
            const visibleElementsCount = Math.floor((target.clientHeight - headerHeightInPx) / defaultRowHeightInPx);

            let start = 0;

            const adjustedScrollOnBottom =
                (target.scrollHeight - (visibleElementsCount + overscan) * defaultRowHeightInPx) * scalar;

            if (scaledScrollTop >= adjustedScrollOnBottom) {
                start = totalResultCount;
            } else {
                start = startElementIndex - overscan;
            }

            setVisibleElementsCount(visibleElementsCount);
            setIsOnBottom(start === totalResultCount);

            debouncedLoadData(start, overscan + visibleElementsCount + overscan);
        };

        const current = tableContainerRef.current;
        current.addEventListener("scroll", handleScroll);

        return () => {
            current.removeEventListener("scroll", handleScroll);
        };
    }, [totalResultCount, overscan, scalar, tableContainerRef, debouncedLoadData]);

    const getRowPositionY = (index: number) => {
        if (scrollTopAfterLoad === 0) {
            return index * defaultRowHeightInPx;
        }

        if (isOnBottom) {
            const x = dataArray.length - visibleElementsCount;
            const r = scrollTopAfterLoad + (index - x) * defaultRowHeightInPx;

            return r;
        }

        return scrollTopAfterLoad + index * defaultRowHeightInPx - overscan * defaultRowHeightInPx;
    };

    const bodyHeightInPx = Math.floor((totalResultCount * defaultRowHeightInPx) / scalar);

    return {
        dataArray,
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
