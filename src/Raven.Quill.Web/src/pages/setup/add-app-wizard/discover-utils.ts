import type { DiscoverColumnResponse, DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";
import { parseConnectionString, type Provider } from "@/pages/setup/add-app-wizard/connection-string";

/** How many source tables one app can process. Unlimited tables are planned. */
export const MAX_SELECTED_TABLES = 10_000;

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

// Mirrors the server's `CdcSinkSchemaDiscovery.ResolveDefaultSchema`
export function resolveDefaultSchema(provider: Provider, connectionString: string): string {
    switch (provider) {
        case "Npgsql":
            return "public";
        case "SqlClient":
            return "dbo";
        case "MySqlConnectorFactory":
            return parseConnectionString(connectionString).values.database || "mysql";
    }
}

export type SourceTableRef = { sourceTableSchema?: string | null; sourceTableName: string };

/** Stable identity for a discovered table ("schema.table"), used as the react-table row id and selection key. */
export function getTableKey(table: SourceTableRef): string {
    return `${table.sourceTableSchema ?? ""}.${table.sourceTableName}`;
}

/** Human-readable table label: "schema.table", the table name alone when there is no schema, or empty without a name. */
export function getTableLabel(table: { sourceTableSchema?: string | null; sourceTableName?: string | null }): string {
    if (!table.sourceTableName) {
        return "";
    }

    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}
