import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";
import { recordFetchStartedAt } from "@/lib/query-fetch-start";

const baseKey = "apps";

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
        cdcErrors: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "cdcErrors", slug],
                queryFn: () => api.cdcErrors(slug),
                // Errors must always reflect the current server state, so never serve them from cache.
                staleTime: 0,
                gcTime: 0,
            }),
        suggestAgentFromData: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "suggestAgentFromData", slug],
                queryFn: async ({ queryKey }) => {
                    recordFetchStartedAt(queryKey);

                    // Suggestions are an optional aid — the wizard works without them — so
                    // failures degrade to an empty list instead of blocking navigation. A missing
                    // AI consent is reported alongside them so the step can name it instead of
                    // blaming the operator's data.
                    const result = await api
                        .suggestAgent(slug, { mode: "from-data", intentPrompt: null })
                        .catch(() => null);

                    return {
                        configurations: result?.status === "Success" ? result.configurations : [],
                        isConsentRequired: result?.status === "ConsentRequired",
                    };
                },
                // A non-empty suggestion is an expensive AI call: never refetch it behind the
                // wizard. An empty one (failure or no candidates) stays stale so the next
                // fetch retries.
                staleTime: (query) => (query.state.data?.configurations.length ? Infinity : 0),
            }),
        aiConnectionStringsList: (slug: string) =>
            queryOptions({
                queryKey: [...APP_AI_CONNECTION_STRINGS_KEY, slug],
                queryFn: () => api.aiConnectionStringsList(slug),
            }),
    };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
