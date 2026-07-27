import { queryOptions } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { tablesSchema, type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { wrapDtoTablesToFormShape } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";

export const SUGGEST_MAP_TABLES_QUERY_KEY = ["setup", "suggestCdc"];

/**
 * The endpoint reasons about the schema the server discovered last - the request carries only the
 * intent prompt - so a suggestion stays valid exactly as long as that schema does. The discovered
 * table list stands in for its identity.
 */
export function computeDiscoveredSchemaKey(discoverResult: DiscoverResponse | null): string {
    return (discoverResult?.tables ?? []).map(getTableKey).sort().join("|");
}

export function suggestMapTablesQuery({
    discoveredSchemaKey,
    intentPrompt,
}: {
    discoveredSchemaKey: string;
    intentPrompt: string;
}) {
    return queryOptions({
        queryKey: [...SUGGEST_MAP_TABLES_QUERY_KEY, discoveredSchemaKey, intentPrompt],
        queryFn: () => suggestMapTables(intentPrompt),
        // The call routinely runs for more than a minute, so the verify step prefetches it and every
        // later read serves that same entry instead of paying for it again. A retry would double the
        // wait, and the cache key already covers every input the answer depends on.
        staleTime: Infinity,
        gcTime: Infinity,
        retry: false,
    });
}

async function suggestMapTables(intentPrompt: string): Promise<AppFormData["mapTables"]["tables"]> {
    const result = await api.services.setup.suggestCdc({ intentPrompt });

    if (result.status !== "Success" || !result.configuration) {
        throw new Error(result.rationale.filter(Boolean).join("\n") || `AI suggestion failed (${result.status}).`);
    }

    try {
        return tablesSchema.parse(wrapDtoTablesToFormShape(result.configuration.tables ?? []));
    } catch {
        throw new Error("The suggested mapping is invalid and could not be loaded into the editor.");
    }
}
