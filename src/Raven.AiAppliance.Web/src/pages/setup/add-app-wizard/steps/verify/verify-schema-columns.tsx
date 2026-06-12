/* eslint-disable react-refresh/only-export-components */
import type { ColumnDef } from "@tanstack/react-table";
import { TriangleAlertIcon } from "lucide-react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { getTableLabel } from "@/pages/setup/add-app-wizard/discover-utils";

const TABLE_NAME_COLUMN: ColumnDef<DiscoverTableResponse> = {
    accessorFn: (table) => getTableLabel(table),
    header: "Table name",
    id: "tableName",
    size: 300,
    cell: ({ row, getValue }) => (
        <span className="flex items-center gap-1.5 font-mono">
            {getValue<string>()}
            {row.original.warnings.length > 0 && (
                <Tooltip>
                    <TooltipTrigger asChild>
                        <TriangleAlertIcon
                            className="size-3.5 shrink-0 text-amber-600 dark:text-amber-400"
                            aria-label="Table warnings"
                        />
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
    size: 100,
    cell: ({ getValue }) => <span className="font-mono">{getValue<string>()}</span>,
};

const COLUMNS_COUNT_COLUMN: ColumnDef<DiscoverTableResponse> = {
    accessorFn: (table) => table.columns.length,
    header: "Columns count",
    id: "columnsCount",
    size: 130,
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
            />
        ),
        cell: ({ row }) => (
            <Checkbox
                checked={row.getIsSelected()}
                onCheckedChange={(value) => row.toggleSelected(!!value)}
                aria-label="Select row"
            />
        ),
        enableSorting: false,
        enableHiding: false,
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
                    <TriangleAlertIcon
                        className="size-4 shrink-0 text-amber-600 dark:text-amber-400"
                        aria-label="Why this table needs configuration"
                    />
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
