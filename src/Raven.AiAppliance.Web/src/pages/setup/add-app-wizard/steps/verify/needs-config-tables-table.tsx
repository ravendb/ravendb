/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { getCoreRowModel, getFilteredRowModel, useReactTable } from "@tanstack/react-table";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { TooltipProvider } from "@/components/shadcn/ui/tooltip";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { NEEDS_CONFIG_COLUMNS } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-columns";

type NeedsConfigTablesTableProps = {
    tables: DiscoverTableResponse[];
    search: string;
};

export function NeedsConfigTablesTable({ tables, search }: NeedsConfigTablesTableProps) {
    // Stable filter-state reference between renders, see the note in verified-tables-table.tsx.
    const columnFilters = useMemo(() => [{ id: "tableName", value: search }], [search]);

    const table = useReactTable({
        columns: NEEDS_CONFIG_COLUMNS,
        data: tables,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: getTableKey,
        state: {
            columnFilters,
        },
    });

    return (
        <TooltipProvider>
            <VirtualDataTable
                table={table}
                columnCount={NEEDS_CONFIG_COLUMNS.length}
                emptyMessage="No tables match the current filter."
                heightInPx={300}
            />
        </TooltipProvider>
    );
}
