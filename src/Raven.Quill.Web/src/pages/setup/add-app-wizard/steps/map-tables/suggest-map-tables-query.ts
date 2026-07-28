import { hashKey, queryOptions, type QueryClient } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { recordFetchStartedAt } from "@/lib/query-fetch-start";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";

export const SUGGEST_MAP_TABLES_QUERY_KEY = ["setup", "suggestCdc"];

type SelectedTables = AppFormData["verifySchema"]["tables"];

/**
 * React Query aborts a fetch the moment its last observer unmounts - but only when the queryFn read
 * the `signal` it hands out. This call must outlive that: it is warmed up a step before anything
 * observes it, it keeps running while the operator steps back and forth, and in development
 * StrictMode's remount would abort and restart it. So the signal is owned here instead, and
 * `cancelAbandonedSuggestions` is the only thing that aborts it.
 */
const abortControllerByQueryHash = new Map<string, AbortController>();

/**
 * The endpoint narrows the schema the server discovered last down to the tables sent with the
 * request, so the discovered table list stands in for that snapshot's identity. It is a deliberately
 * coarse key: re-discovering invalidates suggestions the extra tables never touched, and column
 * changes within a table are not detected at all.
 */
export function computeDiscoveredSchemaKey(discoverResult: DiscoverResponse | null): string {
    return (discoverResult?.tables ?? []).map(getTableKey).sort().join("|");
}

export function suggestMapTablesQuery({
    slug,
    discoveredSchemaKey,
    intentPrompt,
    selectedTables,
}: {
    slug: string;
    discoveredSchemaKey: string;
    intentPrompt: string;
    selectedTables: SelectedTables;
}) {
    return queryOptions({
        queryKey: [
            ...SUGGEST_MAP_TABLES_QUERY_KEY,
            slug,
            discoveredSchemaKey,
            intentPrompt,
            selectedTables.map(getTableKey).sort().join("|"),
        ],
        queryFn: async ({ queryKey }) => {
            recordFetchStartedAt(queryKey);

            const queryHash = hashKey(queryKey);
            const abortController = new AbortController();

            abortControllerByQueryHash.set(queryHash, abortController);

            try {
                return await suggestMapTables(slug, intentPrompt, selectedTables, abortController.signal);
            } finally {
                if (abortControllerByQueryHash.get(queryHash) === abortController) {
                    abortControllerByQueryHash.delete(queryHash);
                }
            }
        },
        // The call routinely runs for more than a minute, so the verify step prefetches it and every
        // later read serves that same entry instead of paying for it again. A retry would double the
        // wait, and the cache key already covers every input the answer depends on.
        staleTime: Infinity,
        gcTime: Infinity,
        retry: false,
    });
}

/** The suggestion query for the mapping inputs the form currently holds. */
export function suggestMapTablesQueryForValues(values: AppFormData, discoverResult: DiscoverResponse | null) {
    return suggestMapTablesQuery({
        slug: values.externalConnection.slug,
        discoveredSchemaKey: computeDiscoveredSchemaKey(discoverResult),
        intentPrompt: values.map.aiPrompt.trim(),
        selectedTables: values.verifySchema.tables,
    });
}

/**
 * Aborts the suggestion requests the wizard has moved on from. The call runs for a minute or more, so
 * an abandoned one - a prefetch answering a prompt the operator has since edited, or one for a
 * mapping source they turned off - would otherwise keep running next to the request actually being
 * waited on. Pass the key the wizard is heading into, or nothing when no suggestion is wanted at all.
 */
export function cancelAbandonedSuggestions(queryClient: QueryClient, keptQueryKey?: readonly unknown[]): void {
    const keptQueryHash = keptQueryKey && hashKey(keptQueryKey);

    // Reverts each abandoned entry to its pre-fetch state first, so the abort below is swallowed as a
    // cancellation instead of landing in the cache as a failed suggestion.
    void queryClient.cancelQueries({
        queryKey: SUGGEST_MAP_TABLES_QUERY_KEY,
        predicate: (query) => query.queryHash !== keptQueryHash,
    });

    for (const [queryHash, abortController] of abortControllerByQueryHash) {
        if (queryHash !== keptQueryHash) {
            abortController.abort();
            abortControllerByQueryHash.delete(queryHash);
        }
    }
}

async function suggestMapTables(
    slug: string,
    intentPrompt: string,
    selectedTables: SelectedTables,
    signal: AbortSignal,
): Promise<AppFormData["mapTables"]["tables"]> {
    const result = await api.services.setupSuggestions.suggestCdc({ slug, intentPrompt, selectedTables }, signal);

    if (result.status !== "Success" || !result.configuration) {
        throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    try {
        return tablesSchema.parse(wrapDtoTablesToFormShape(result.configuration.tables ?? []));
    } catch (error) {
        console.error("The AI-suggested mapping failed validation.", error);
        throw new Error("The suggested mapping is invalid and could not be loaded into the editor.", {
            cause: error,
        });
    }
}
