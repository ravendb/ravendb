import type { CdcSinkTableConfig } from "@/api/generated/server-api";
import { tablesSchema } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import {
    mapFormTablesToDto,
    wrapDtoTablesToFormShape,
} from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-dto";
import type { FormRootTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-types";

// Bridges the table editor's form state and its raw JSON representation, which is the canonical
// CdcSinkTableConfig[] DTO (the same shape exported configs use). Only the tables are exposed here;
// the connection details stay in the form.

export function serializeFormTablesToRaw(tables: FormRootTable[]): string {
    return JSON.stringify(mapFormTablesToDto(tables), null, 2);
}

export function parseRawTablesToForm(rawContent: string): FormRootTable[] {
    let parsed: unknown;

    try {
        parsed = JSON.parse(rawContent);
    } catch {
        throw new Error("The raw configuration is not valid JSON.");
    }

    if (!Array.isArray(parsed)) {
        throw new Error("The raw configuration must be a JSON array of tables.");
    }

    const result = tablesSchema.safeParse(wrapDtoTablesToFormShape(parsed as CdcSinkTableConfig[]));

    if (!result.success) {
        throw new Error(result.error.issues[0]?.message ?? "The raw configuration does not match the table shape.");
    }

    return result.data;
}

/** Non-throwing variant for the editor's live sync, where invalid intermediate JSON is expected. */
export function tryParseRawTablesToForm(rawContent: string): FormRootTable[] | null {
    try {
        return parseRawTablesToForm(rawContent);
    } catch {
        return null;
    }
}
