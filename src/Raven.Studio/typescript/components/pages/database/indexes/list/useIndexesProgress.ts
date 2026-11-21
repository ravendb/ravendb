import { Dispatch, useEffect, useRef, useState } from "react";
import { IndexSharedInfo } from "components/models/indexes";
import { IndexesStatsReducerAction } from "components/pages/database/indexes/list/IndexesStatsReducer";
import { useServices } from "hooks/useServices";
import messagePublisher from "common/messagePublisher";
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;

const locationKey = (location: databaseLocationSpecifier) => `${location.nodeTag}/${location.shardNumber ?? ""}`;

export function useIndexesProgress(
    databaseName: string,
    indexes: IndexSharedInfo[],
    dispatch: Dispatch<IndexesStatsReducerAction>
) {
    const { indexesService } = useServices();

    const indexesWithExactProgressRef = useRef<Set<string>>(new Set());
    const [indexesWithExactProgress, setIndexesWithExactProgress] = useState<string[]>([]);

    const staleIndexNamesRef = useRef<string[]>([]);
    useEffect(() => {
        // staleness can differ between nodes - an index counts as stale when at least one node reports it stale
        staleIndexNamesRef.current = indexes
            .filter((x) => x.nodesInfo.some((n) => n.details?.stale))
            .map((x) => x.name);
    }, [indexes]);

    // the progress requests are not sequenced by the server - a late response must not overwrite a newer one
    const progressRequestIdsRef = useRef(new Map<string, number>());

    const startProgressRequest = (key: string) => {
        const requestId = (progressRequestIdsRef.current.get(key) ?? 0) + 1;
        progressRequestIdsRef.current.set(key, requestId);

        return () => progressRequestIdsRef.current.get(key) === requestId;
    };

    // an index toggled while a request is in flight already has newer progress, so it is skipped
    const trackIndexesToggledSinceNow = (nodeKey: string) => {
        const requestIdsAtStart = new Map(progressRequestIdsRef.current);

        return () =>
            Array.from(progressRequestIdsRef.current)
                .filter(([key, id]) => key.startsWith(`${nodeKey}/`) && requestIdsAtStart.get(key) !== id)
                .map(([key]) => key.slice(nodeKey.length + 1));
    };

    // the exact calculation is expensive, so it runs only for the stale indexes the user asked for
    const getExactProgressIndexNames = () =>
        staleIndexNamesRef.current.filter((x) => indexesWithExactProgressRef.current.has(x));

    // the exact calculation has no server time limit, so a node runs one at a time: a new request waits for the previous one
    const exactRequestsRef = useRef(new Map<string, Promise<IndexProgress[]>>());

    const getExactProgress = (location: databaseLocationSpecifier, indexNames: string[]) => {
        const nodeKey = locationKey(location);
        const previousRequest = exactRequestsRef.current.get(nodeKey);

        const request = (async () => {
            await Promise.allSettled([previousRequest]);
            return indexesService.getProgress(databaseName, location, indexNames, true);
        })();
        exactRequestsRef.current.set(nodeKey, request);

        const removeIfLatest = () => {
            if (exactRequestsRef.current.get(nodeKey) === request) {
                exactRequestsRef.current.delete(nodeKey);
            }
        };
        request.then(removeIfLatest, removeIfLatest);

        return request;
    };

    const fetchProgress = async (location: databaseLocationSpecifier) => {
        const nodeKey = locationKey(location);
        const isLatestRequest = startProgressRequest(nodeKey);
        const indexesToggledSinceStart = trackIndexesToggledSinceNow(nodeKey);
        const exactNames = getExactProgressIndexNames();

        try {
            // not scoped, so the server also reports the indexes that became stale since the last stats refresh;
            // the indexes with exact progress are skipped, the exact request covers them
            const progress = await indexesService.getProgress(databaseName, location, null, false, exactNames);

            if (!isLatestRequest()) {
                return;
            }

            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
                exclude: [...exactNames, ...indexesToggledSinceStart()],
            });
        } catch (e) {
            if (!isLatestRequest()) {
                return;
            }

            dispatch({
                type: "ProgressLoadError",
                error: e,
                location,
            });
        }
    };

    const fetchExactProgress = async (location: databaseLocationSpecifier) => {
        const nodeKey = locationKey(location);
        const exactNames = getExactProgressIndexNames();

        if (exactNames.length === 0 || exactRequestsRef.current.has(nodeKey)) {
            return;
        }

        const indexesToggledSinceStart = trackIndexesToggledSinceNow(nodeKey);
        const exactNamesToApply = () => exactNames.filter((x) => !indexesToggledSinceStart().includes(x));

        try {
            const progress = await getExactProgress(location, exactNames);

            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
                scope: exactNamesToApply(),
            });
        } catch (e) {
            // fall back to the estimated counts, so the failing exact request is not repeated on every refresh
            exactNamesToApply().forEach((name) => setExactProgress(name, false));
            messagePublisher.reportError("Failed to get exact index progress", e?.responseText);
        }
    };

    const fetchSingleIndexProgress = async (
        index: IndexSharedInfo,
        location: databaseLocationSpecifier,
        exact: boolean
    ) => {
        // keyed per index, so it does not supersede the periodic refresh of the other indexes on the node
        const nodeKey = locationKey(location);
        const isLatestIndexRequest = startProgressRequest(`${nodeKey}/${index.name}`);
        const nodeRequestId = progressRequestIdsRef.current.get(nodeKey);
        // an estimated periodic refresh started later is newer, while an exact index is excluded from it
        const isLatestRequest = () =>
            isLatestIndexRequest() && (exact || progressRequestIdsRef.current.get(nodeKey) === nodeRequestId);

        try {
            const progress = exact
                ? await getExactProgress(location, [index.name])
                : await indexesService.getProgress(databaseName, location, [index.name], false);

            if (!isLatestRequest()) {
                return;
            }

            dispatch({
                type: "ProgressLoaded",
                progress,
                location,
                scope: [index.name],
            });
        } catch (e) {
            if (isLatestRequest()) {
                throw e;
            }
        }
    };

    const setExactProgress = (indexName: string, exact: boolean) => {
        const set = indexesWithExactProgressRef.current;

        if (exact) {
            set.add(indexName);
        } else {
            set.delete(indexName);
        }

        setIndexesWithExactProgress(Array.from(set));
    };

    const toggleExactProgress = async (index: IndexSharedInfo) => {
        const exact = !indexesWithExactProgressRef.current.has(index.name);

        // the toggle is applied up front so a concurrent refresh already uses the requested accuracy,
        // and reverted when the fetch fails so the link matches the displayed counts
        setExactProgress(index.name, exact);

        // the toggle is per index, so every node where the index is stale is refetched
        const results = await Promise.allSettled(
            index.nodesInfo
                .filter((x) => x.details?.stale)
                .map((x) => fetchSingleIndexProgress(index, x.location, exact))
        );

        const failure = results.find((x) => x.status === "rejected");
        if (failure) {
            setExactProgress(index.name, !exact);
            throw failure.reason;
        }
    };

    const refreshProgress = (location: databaseLocationSpecifier) => {
        fetchProgress(location);
        fetchExactProgress(location);
    };

    return {
        refreshProgress,
        indexesWithExactProgress,
        toggleExactProgress,
    };
}
