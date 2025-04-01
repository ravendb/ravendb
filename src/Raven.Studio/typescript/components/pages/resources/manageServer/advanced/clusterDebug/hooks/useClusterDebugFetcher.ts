import { virtualTableConstants } from "components/common/virtualTable/utils/virtualTableConstants";
import { useServices } from "components/hooks/useServices";
import { useEffect, useRef, useState } from "react";
import { useAsync, useAsyncCallback } from "react-async-hook";

interface useClusterDebugFetcherProps {
    nodeTag: string;
    dependencies: any[];
}

interface ClusterDebugFetcherResult {
    dataArray: Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry[];
    commitIndex: number;
    componentProps: {
        tableContainerRef: React.RefObject<HTMLDivElement>;
        isLoading: boolean;
    };
}
export function useClusterDebugFetcher(props: useClusterDebugFetcherProps): ClusterDebugFetcherResult {
    const tableContainerRef = useRef<HTMLDivElement>(null);

    const { manageServerService } = useServices();

    const [dataArray, setDataArray] = useState<Raven.Server.Rachis.RachisConsensus.RachisDebugLogEntry[]>([]);
    const [hasMore, setHasMore] = useState<boolean>(true);
    const [nextIndexToFetch, setNextIndexToFetch] = useState<number>(null);
    const [commitIndex, setCommitIndex] = useState<number>(null);

    const asyncLoadInitialData = useAsync(async () => {
        try {
            const result = await manageServerService.getClusterLog(props.nodeTag, undefined, 1024);

            const log = result.Log;
            const data = log.Logs;
            setCommitIndex(log.CommitIndex);

            const logLength = result.Log.Logs.length;
            const hasMore = logLength !== result.Log.TotalEntries;

            setHasMore(hasMore);

            setNextIndexToFetch(log.Logs.length ? log.Logs[log.Logs.length - 1].Index - 1 : 0);

            setDataArray(data);
        } catch (_) {
            setCommitIndex(null);
            setHasMore(false);
            setNextIndexToFetch(null);
            setDataArray([]);
        }
    }, props.dependencies);

    const asyncLoadData = useAsyncCallback(async () => {
        const chuckSize = 1001;

        const result = await manageServerService.getClusterLog(props.nodeTag, nextIndexToFetch, chuckSize);

        setCommitIndex(result.Log.CommitIndex);

        const hasMore = result.Log.Logs.length === chuckSize;
        setHasMore(hasMore);

        if (hasMore) {
            // truncate last item
            const lastItem = result.Log.Logs.pop();
            setNextIndexToFetch(lastItem.Index);
        }

        setDataArray((prev) => [...prev, ...result.Log.Logs]);
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
        commitIndex,
        componentProps: {
            tableContainerRef,
            isLoading: asyncLoadInitialData.loading || asyncLoadData.loading,
        },
    };
}

const { defaultRowHeightInPx } = virtualTableConstants;
