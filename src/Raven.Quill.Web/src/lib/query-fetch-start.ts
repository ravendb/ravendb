import { hashKey, type QueryClient, type QueryFilters } from "@tanstack/react-query";

const startedAtByHashedKey = new Map<string, number>();

/**
 * React Query does not expose when a fetch was dispatched, but the AI progress timers need it: the
 * suggestion calls are prefetched a wizard step earlier, so a timer that starts on mount would
 * undercount by however long the call has already been running. Call this first thing in the
 * queryFn and read it back with `getFetchStartedAt`.
 */
export function recordFetchStartedAt(queryKey: readonly unknown[]): void {
    startedAtByHashedKey.set(hashKey(queryKey), Date.now());
}

export function getFetchStartedAt(queryKey: readonly unknown[]): number | undefined {
    return startedAtByHashedKey.get(hashKey(queryKey));
}

function clearFetchStartedAt(queryKey: readonly unknown[]): void {
    startedAtByHashedKey.delete(hashKey(queryKey));
}

export function dropQueries(queryClient: QueryClient, filters: QueryFilters): void {
    for (const query of queryClient.getQueryCache().findAll(filters)) {
        clearFetchStartedAt(query.queryKey);
    }

    queryClient.removeQueries(filters);
}
