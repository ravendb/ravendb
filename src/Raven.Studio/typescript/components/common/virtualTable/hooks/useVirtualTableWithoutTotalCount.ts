import { useCallback, useEffect, useRef, useState } from "react";
import { useAsyncCallback } from "react-async-hook";
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
    // when any of these change, the table resets and refetches page 0 (also covers the initial load)
    reloadDependencies?: unknown[];
}

export function useVirtualTableWithoutTotalCount<T extends PagedResultWithoutCount>({
    fetchData,
    initialOverscan = 50,
    reloadDependencies = [],
}: useVirtualTableWithoutTotalCountProps<T>) {
    const tableContainerRef = useRef<HTMLDivElement>(null);

    const initialItemsCount = Math.ceil(window.innerHeight / defaultRowHeightInPx) + initialOverscan;

    const [dataArray, setDataArray] = useState<T["items"]>([]);

    // refs (not state) so the mount-time scroll handler always sees current values
    const fetchDataRef = useRef(fetchData);
    fetchDataRef.current = fetchData;
    const nextItemToFetchIndexRef = useRef(0);
    const hasMoreRef = useRef(true);

    // synchronous in-flight guard; asyncLoadData.loading only updates after a re-render
    const isFetchingRef = useRef(false);

    // incremented on every reset so in-flight appends started before the reset discard their results
    const generationRef = useRef(0);

    const asyncLoadData = useAsyncCallback(async (reset: boolean) => {
        const generation = reset ? ++generationRef.current : generationRef.current;
        isFetchingRef.current = true;

        try {
            const skip = reset ? 0 : nextItemToFetchIndexRef.current;
            const result = await fetchDataRef.current(skip, initialItemsCount);

            if (generation !== generationRef.current) {
                // a reset happened while this fetch was in flight - discard the stale result
                return;
            }

            hasMoreRef.current = result.items.length === initialItemsCount;
            nextItemToFetchIndexRef.current = skip + result.items.length;
            setDataArray((prev) => (reset ? result.items : [...prev, ...result.items]));
        } finally {
            // a stale call must not clear the guard while a newer reset fetch is still in flight
            if (generation === generationRef.current) {
                isFetchingRef.current = false;
            }
        }
    });

    const asyncLoadDataRef = useRef(asyncLoadData);
    asyncLoadDataRef.current = asyncLoadData;

    const reload = useCallback(async () => {
        hasMoreRef.current = true;
        // optional call: jsdom's HTMLElement has no scrollTo
        tableContainerRef.current?.scrollTo?.({ top: 0 });
        await asyncLoadDataRef.current.execute(true);
    }, []);

    // single load path: runs on mount and whenever a reload dependency changes
    useEffect(() => {
        reload();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, reloadDependencies);

    // Handle scroll
    useEffect(() => {
        if (!tableContainerRef.current) {
            return;
        }

        const handleScroll = (e: Event) => {
            if (!hasMoreRef.current || isFetchingRef.current) {
                return;
            }

            const target = e.target as HTMLDivElement;
            const positionToFetch = target.scrollHeight - target.clientHeight - defaultRowHeightInPx;

            if (target.scrollTop >= positionToFetch) {
                asyncLoadDataRef.current.execute(false);
            }
        };

        const current = tableContainerRef.current;
        current.addEventListener("scroll", handleScroll);

        return () => {
            current.removeEventListener("scroll", handleScroll);
        };
    }, []);

    return {
        dataArray,
        reload,
        componentProps: {
            tableContainerRef,
            isLoading: asyncLoadData.loading,
        },
    };
}

const { defaultRowHeightInPx } = virtualTableConstants;
