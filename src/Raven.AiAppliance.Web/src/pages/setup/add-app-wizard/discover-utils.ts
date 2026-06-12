import type { DiscoverColumnResponse, DiscoverResponse, DiscoverTableResponse } from "@/api/generated/server-api";

/** A discovered table can be used when discovery succeeded, the table is supported, and CDC
 * is either already enabled on it or the connecting user has permission to set CDC up. */
export function isTableSupported(discoverResult: DiscoverResponse | null, table: DiscoverTableResponse): boolean {
    return Boolean(
        discoverResult?.success &&
        !table.unsupportedReason &&
        (table.isCdcEnabled || discoverResult.hasPermissionToSetup),
    );
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
