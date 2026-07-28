import { queryOptions } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";

export const SUGGEST_MAP_TABLES_QUERY_KEY = ["setup", "suggestCdc"];

type SelectedTables = AppFormData["verifySchema"]["tables"];

/**
 * The endpoint narrows the schema the server discovered last down to the tables sent with the
 * request, so a suggestion stays valid exactly as long as that schema does. The discovered table
 * list stands in for its identity.
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
        queryFn: () => suggestMapTables(slug, intentPrompt, selectedTables),
        // The call routinely runs for more than a minute, so the verify step prefetches it and every
        // later read serves that same entry instead of paying for it again. A retry would double the
        // wait, and the cache key already covers every input the answer depends on.
        staleTime: Infinity,
        gcTime: Infinity,
        retry: false,
    });
}

async function suggestMapTables(
    slug: string,
    intentPrompt: string,
    selectedTables: SelectedTables,
): Promise<AppFormData["mapTables"]["tables"]> {
    const result = await api.services.setup.suggestCdc({ slug, intentPrompt, selectedTables });

    if (result.status !== "Success" || !result.configuration) {
        throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    try {
        return tablesSchema.parse(wrapDtoTablesToFormShape(result.configuration.tables ?? []));
    } catch {
        throw new Error("The suggested mapping is invalid and could not be loaded into the editor.");
    }
}
