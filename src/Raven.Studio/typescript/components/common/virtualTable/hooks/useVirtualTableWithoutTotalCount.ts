import { useEffect, useRef, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";
import { virtualTableConstants } from "../utils/virtualTableConstants";

// Use it along with VirtualTable component

// It is possible to exceed the maximum height of an element in the browser,
// but for 223 695 rows (firefox limit) it would require scrolling to the bottom over 2 000 times,
// so we can ignore this limitation

type PagedResultWithoutCount = Omit<pagedResult<unknown>, "totalResultCount">;

type FetchData<T extends PagedResultWithoutCount> = (skip: number, take: number) => Promise<T>;

interface useVirtualTableWithoutTotalCountProps<T extends PagedResultWithoutCount> {
    fetchData: FetchData<T>;
    initialOverscan?: number;
    dependencies?: any[];
}

export function useVirtualTableWithoutTotalCount<T extends PagedResultWithoutCount>({
    fetchData,
    initialOverscan = 50,
    dependencies = [],
}: useVirtualTableWithoutTotalCountProps<T>) {
    const tableContainerRef = useRef<HTMLDivElement>(null);

    const initialItemsCount = Math.ceil(window.innerHeight / defaultRowHeightInPx) + initialOverscan;

    const [dataArray, setDataArray] = useState<T["items"]>([]);

    const [nextItemToFetchIndex, setNextItemToFetchIndex] = useState<number>(0);
    const [hasMore, setHasMore] = useState<boolean>(true);

    const asyncLoadInitialData = useAsync(async () => {
        const result = await fetchData(0, initialItemsCount);

        setHasMore(result.items.length === initialItemsCount);
        setNextItemToFetchIndex(nextItemToFetchIndex + initialItemsCount);
        setDataArray(result.items);
    }, dependencies);

    const asyncLoadData = useAsyncCallback(async () => {
        const result = await fetchData(nextItemToFetchIndex, initialItemsCount);

        setHasMore(result.items.length === initialItemsCount);
        setNextItemToFetchIndex(nextItemToFetchIndex + initialItemsCount);
        setDataArray((prev) => [...prev, ...result.items]);
    });

    // Handle scroll
    useEffect(() => {
        if (!tableContainerRef.current) {
            return;
        }
        let isFetching = false;

        const handleScroll = async (e: Event) => {
            if (!hasMore) {
                return;
            }

            const target = e.target as HTMLDivElement;
            const positionToFetch = target.scrollHeight - target.clientHeight - defaultRowHeightInPx;

            if (target.scrollTop >= positionToFetch) {
                if (isFetching) {
                    return;
                }

                isFetching = true;
                await asyncLoadData.execute();
            } else {
                isFetching = false;
            }
        };

        const current = tableContainerRef.current;
        current.addEventListener("scroll", handleScroll);

        return () => {
            current.removeEventListener("scroll", handleScroll);
        };
    }, [hasMore]);

    return {
        dataArray,
        componentProps: {
            tableContainerRef,
            isLoading: asyncLoadInitialData.loading || asyncLoadData.loading,
        },
    };
}

const { defaultRowHeightInPx } = virtualTableConstants;
