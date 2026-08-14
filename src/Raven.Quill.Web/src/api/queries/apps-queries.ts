import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";
import { recordFetchStartedAt } from "@/lib/query-fetch-start";

const baseKey = "apps";

// The overview presents sync as a live reading, so its two queries refresh on their own while the
// tab is open. They poll together: refreshing the status and the batch shape while the error count
// and the failing verdict beside them sat stale would be worse than not refreshing at all. React
// Query holds the timer while the tab is in the background.
const SYNC_POLL_INTERVAL_IN_MS = 15_000;

// Partial key covering every app's connection strings list, so mutations on the
// (server-wide) connection strings can invalidate all of them at once.
export const APP_AI_CONNECTION_STRINGS_KEY = [baseKey, "aiConnectionStringsList"] as const;

export function createAppsQueries(api: ServerApi["apps"]) {
    return {
        detail: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "detail", slug],
                queryFn: () => api.detail(slug),
            }),
        list: () =>
            queryOptions({
                queryKey: [baseKey, "list"],
                queryFn: () => api.list(),
            }),
        cdcGet: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "cdcGet", slug],
                queryFn: () => api.cdcGet(slug),
            }),
        cdcPerformance: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "cdcPerformance", slug],
                queryFn: () => api.cdcPerformance(slug),
                // Sync health is a live reading, so it refetches on every mount. It keeps its cache
                // entry, unlike cdcErrors, so revisiting shows the last status instead of a spinner.
                staleTime: 0,
                refetchInterval: SYNC_POLL_INTERVAL_IN_MS,
            }),
        cdcErrors: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "cdcErrors", slug],
                queryFn: () => api.cdcErrors(slug),
                // Errors must always reflect the current server state, so never serve them from cache.
                staleTime: 0,
                gcTime: 0,
                refetchInterval: SYNC_POLL_INTERVAL_IN_MS,
            }),
        suggestAgentFromData: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "suggestAgentFromData", slug],
                queryFn: async ({ queryKey }) => {
                    recordFetchStartedAt(queryKey);

                    // Suggestions are an optional aid — the wizard works without them — so
                    // failures degrade to an empty list instead of blocking navigation.
                    const result = await api
                        .suggestAgent(slug, { mode: "from-data", intentPrompt: null })
                        .catch(() => null);

                    return result?.status === "Success" ? result.configurations : [];
                },
                // A non-empty suggestion is an expensive AI call: never refetch it behind the
                // wizard. An empty one (failure or no candidates) stays stale so the next
                // fetch retries.
                staleTime: (query) => ((query.state.data?.length ?? 0) > 0 ? Infinity : 0),
            }),
        aiConnectionStringsList: (slug: string) =>
            queryOptions({
                queryKey: [...APP_AI_CONNECTION_STRINGS_KEY, slug],
                queryFn: () => api.aiConnectionStringsList(slug),
            }),
    };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
