/* eslint-disable react-refresh/only-export-components */
import type { ColumnDef } from "@tanstack/react-table";
import { TriangleAlertIcon } from "lucide-react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";
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
                        <Button variant="link" aria-label="Table warnings">
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

export const VERIFIED_COLUMNS: ColumnDef<DiscoverTableResponse>[] = [
    {
        id: "select",
        header: ({ table }) => (
            <Checkbox
                checked={table.getIsAllPageRowsSelected() || (table.getIsSomePageRowsSelected() && "indeterminate")}
                onCheckedChange={(value) => table.toggleAllPageRowsSelected(!!value)}
                aria-label="Select all"
                disabled={table.options.enableRowSelection === false}
            />
        ),
        cell: ({ row }) => (
            <Checkbox
                checked={row.getIsSelected()}
                onCheckedChange={(value) => row.toggleSelected(!!value)}
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

    return "This table cannot be configured for CDC.";
}
