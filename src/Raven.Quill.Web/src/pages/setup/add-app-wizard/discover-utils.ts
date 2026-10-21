import type { DiscoverColumnResponse, DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";

/** How many source tables one app can process. Beta-only limit of the AI service; unlimited tables are planned. */
export const MAX_SELECTED_TABLES = 64;

/** A discovered table can be used when discovery succeeded, the table is supported, and CDC
 * is either already enabled on it or the connecting user has permission to set CDC up. */
export function isTableSupported(discoverResult: DiscoverResponse | null, table: DiscoverTableResponse): boolean {
    return Boolean(
        discoverResult?.success &&
        !table.unsupportedReason &&
        (table.isCdcEnabled || discoverResult.hasPermissionToSetup),
    );
}

export function hasTableWarnings(table: DiscoverTableResponse): boolean {
    return table.warnings.length > 0;
}

export type WarningsFilter = "all" | "no-warnings" | "warnings";

export function matchesWarningsFilter(table: DiscoverTableResponse, filter: WarningsFilter): boolean {
    switch (filter) {
        case "all":
            return true;
        case "no-warnings":
            return !hasTableWarnings(table);
        case "warnings":
            return hasTableWarnings(table);
    }
}

export function matchesTableSearch(table: DiscoverTableResponse, search: string): boolean {
    return getTableLabel(table).toLowerCase().includes(search.toLowerCase());
}

/** A table without CDC enabled yet reports all columns as non-capturable; when the user has
 * permission to set CDC up, every discovered column of such a table is still eligible. */
export function isColumnSupported(
    discoverResult: DiscoverResponse | null,
    table: DiscoverTableResponse,
    column: DiscoverColumnResponse,
): boolean {
    return column.isCdcCapturable || Boolean(discoverResult?.hasPermissionToSetup && !table.isCdcEnabled);
}

/** Stable identity for a discovered table ("schema.table"), used as the react-table row id and selection key. */
export function getTableKey(table: Pick<DiscoverTableResponse, "sourceTableName" | "sourceTableSchema">): string {
    return `${table.sourceTableSchema ?? ""}.${table.sourceTableName}`;
}

/** Human-readable table label: "schema.table", or just the table name when there is no schema. */
export function getTableLabel(table: DiscoverTableResponse): string {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}
