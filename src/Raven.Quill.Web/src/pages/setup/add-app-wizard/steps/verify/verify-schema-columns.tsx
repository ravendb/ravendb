/* eslint-disable react-refresh/only-export-components */
// The react-table instance keeps one identity for its whole lifetime and mutates its state in
// place, so a compiled renderer would be memoized against a prop that never changes.
"use no memo";

import type { RefObject } from "react";
import type { ColumnDef, Table } from "@tanstack/react-table";
import { TriangleAlertIcon } from "lucide-react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { countSelectedRows, getRangeSelection, setRowsSelected } from "@/components/table/row-range-selection";
import { getTableLabel, MAX_SELECTED_TABLES } from "@/pages/setup/add-app-wizard/discover-utils";
import { Button } from "@/components/shadcn/ui/button";

const TABLE_NAME_COLUMN: ColumnDef<DiscoverTableResponse> = {
    accessorFn: (table) => getTableLabel(table),
    header: "Table name",
    id: "tableName",
    cell: ({ row, getValue }) => (
        <span className="flex min-w-0 items-center gap-1.5 font-mono">
            <span className="truncate">{getValue<string>()}</span>
            {row.original.warnings.length > 0 && (
                <Tooltip>
                    <TooltipTrigger asChild>
                        <Button variant="link" aria-label="Table warnings" className="cursor-default px-0">
                            <TriangleAlertIcon
                                className="size-3.5 shrink-0 text-amber-600 dark:text-amber-400"
                                aria-hidden="true"
                            />
                        </Button>
                    </TooltipTrigger>
                    <TooltipContent>
                        <MessageTooltipBody messages={row.original.warnings} />
                    </TooltipContent>
                </Tooltip>
            )}
        </span>
    ),
};

const PRIMARY_KEY_COLUMN: ColumnDef<DiscoverTableResponse> = {
    accessorFn: (table) => table.primaryKeyColumns.join(", "),
    header: "Primary key",
    id: "primaryKey",
    cell: ({ getValue }) => <span className="font-mono">{getValue<string>()}</span>,
};

const COLUMNS_COUNT_COLUMN: ColumnDef<DiscoverTableResponse> = {
    accessorFn: (table) => table.columns.length,
    header: "Columns count",
    id: "columnsCount",
    cell: ({ getValue }) => <span className="tabular-nums">{getValue<number>()}</span>,
};

/**
 * Columns of the verified tables table. Built per mounted table rather than shared as a constant so
 * the select column can write the row of the last plain click into `anchorRowIdRef`: a shift-click
 * spans the rows between that anchor and the clicked row, and the table previews the same span on
 * hover. The clicked row decides whether the span is selected or cleared - see `getRangeSelection`.
 */
export function createVerifiedColumns(anchorRowIdRef: RefObject<string | null>): ColumnDef<DiscoverTableResponse>[] {
    return [
        {
            id: "select",
            header: ({ table }) => <SelectAllCheckbox table={table} />,
            cell: ({ row, table }) => (
                <Checkbox
                    checked={row.getIsSelected()}
                    // Radix skips its own toggle once a click handler prevents the default, which is
                    // what turns a shift-click into a range selection instead of a single toggle.
                    onClick={(event) => {
                        const range = event.shiftKey
                            ? getRangeSelection(table, anchorRowIdRef.current, row.id, MAX_SELECTED_TABLES)
                            : null;

                        if (range === null || range.rows.length === 0) {
                            return;
                        }

                        event.preventDefault();
                        setRowsSelected(table, range.rows, range.isSelecting, MAX_SELECTED_TABLES);
                    }}
                    onCheckedChange={(value) => {
                        row.toggleSelected(!!value);
                        anchorRowIdRef.current = row.id;
                    }}
                    aria-label="Select row"
                    disabled={!row.getCanSelect()}
                />
            ),
            enableSorting: false,
            enableHiding: false,
            enableResizing: false,
            size: 40,
        },
        TABLE_NAME_COLUMN,
        PRIMARY_KEY_COLUMN,
        COLUMNS_COUNT_COLUMN,
    ];
}

/**
 * Selects or clears every currently visible row. Not react-table's own `toggleAllPageRowsSelected`:
 * that one reports "all selected" as soon as the only selectable rows are the selected ones, which
 * is exactly the state the selection limit puts the table in.
 */
function SelectAllCheckbox({ table }: { table: Table<DiscoverTableResponse> }) {
    const rows = table.getRowModel().rows;
    const { rowSelection } = table.getState();
    const selectedCount = rows.filter((row) => rowSelection[row.id]).length;
    // Radix reads a click on an indeterminate box as a request to select, which the limit cannot
    // grant - clearing the visible rows is then the only thing left for the click to do.
    const hasRoomForMore = countSelectedRows(rowSelection) < MAX_SELECTED_TABLES;

    return (
        <Checkbox
            checked={selectedCount === 0 ? false : selectedCount === rows.length ? true : "indeterminate"}
            onCheckedChange={(value) =>
                setRowsSelected(table, rows, Boolean(value) && hasRoomForMore, MAX_SELECTED_TABLES)
            }
            aria-label="Select all"
            // Nothing left to toggle when the selection is read-only, or when the limit is reached
            // and none of the visible rows is selected.
            disabled={!rows.some((row) => row.getCanSelect())}
        />
    );
}

export const NEEDS_CONFIG_COLUMNS: ColumnDef<DiscoverTableResponse>[] = [
    TABLE_NAME_COLUMN,
    PRIMARY_KEY_COLUMN,
    COLUMNS_COUNT_COLUMN,
    {
        id: "reason",
        header: "Reason",
        size: 80,
        cell: ({ row }) => (
            <Tooltip>
                <TooltipTrigger asChild>
                    <Button variant="link" aria-label="Why this table needs configuration">
                        <TriangleAlertIcon
                            className="size-4 shrink-0 text-amber-600 dark:text-amber-400"
                            aria-hidden="true"
                        />
                    </Button>
                </TooltipTrigger>
                <TooltipContent>{getNeedsConfigurationReason(row.original)}</TooltipContent>
            </Tooltip>
        ),
    },
];

function MessageTooltipBody({ messages }: { messages: string[] }) {
    return (
        <span className="grid gap-1">
            {messages.map((message, index) => (
                <span key={index}>{message}</span>
            ))}
        </span>
    );
}

function getNeedsConfigurationReason(table: DiscoverTableResponse) {
    if (table.unsupportedReason) {
        return table.unsupportedReason;
    }

    if (!table.isCdcEnabled) {
        return "CDC is not enabled. Ask a database administrator to enable CDC for this table.";
    }

    return "This table cannot be synced.";
}
