import type { ColumnDef } from "@tanstack/react-table";

/**
 * Header labels of a react-table column set, for feeding `TableSkeleton`. A header rendered by a
 * function has no label to reuse, so that column is left unlabelled rather than guessed at.
 */
export function getColumnHeaderLabels<TData>(columns: ColumnDef<TData>[]): string[] {
    return columns.map((column) => (typeof column.header === "string" ? column.header : ""));
}
