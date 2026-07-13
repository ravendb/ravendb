/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { getCoreRowModel, getFilteredRowModel, useReactTable, type RowSelectionState } from "@tanstack/react-table";
import { XIcon } from "lucide-react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { TooltipProvider } from "@/components/shadcn/ui/tooltip";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { cn } from "@/lib/utils";
import { getTableKey } from "@/pages/setup/add-app-wizard/discover-utils";
import { VERIFIED_COLUMNS } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-columns";

type VerifiedTablesTableProps = {
    tables: DiscoverTableResponse[];
    /** Total number of discovered tables, shown in the "x out of y" selection summary. */
    totalTableCount: number;
    search: string;
    rowSelection: RowSelectionState;
    onRowSelectionChange: (selection: RowSelectionState) => void;
    /** When an imported configuration is locked, the selection is read-only. */
    disabled?: boolean;
};

export function VerifiedTablesTable({
    tables,
    totalTableCount,
    search,
    rowSelection,
    onRowSelectionChange,
    disabled,
}: VerifiedTablesTableProps) {
    // react-table needs a stable filter-state reference between renders; a fresh identity on
    // every render makes it recompute row models and queue state resets, ending in a re-render
    // loop. "use no memo" opts this file out of the React Compiler (incompatible with react-table),
    // so the explicit useMemo matters. The `tables` data array is memoized by the parent.
    const columnFilters = useMemo(() => [{ id: "tableName", value: search }], [search]);

    const table = useReactTable({
        columns: VERIFIED_COLUMNS,
        data: tables,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: getTableKey,
        enableRowSelection: !disabled,
        onRowSelectionChange: (updaterOrValue) => {
            const value = typeof updaterOrValue === "function" ? updaterOrValue(rowSelection) : updaterOrValue;
            onRowSelectionChange(value);
        },
        state: {
            rowSelection,
            columnFilters,
        },
    });

    const selectedCount = tables.filter((verifiedTable) => rowSelection[getTableKey(verifiedTable)]).length;

    return (
        <TooltipProvider>
            <VirtualDataTable
                table={table}
                columnCount={VERIFIED_COLUMNS.length}
                emptyMessage="No tables match the current filter."
                heightInPx={300}
                className={cn(selectedCount > 0 && "mb-4")}
                getRowState={(rowId) => (rowSelection[rowId] ? "selected" : "")}
                overlay={
                    selectedCount > 0 && (
                        <div className="absolute bottom-0 left-1/2 flex -translate-x-1/2 translate-y-1/2 items-center gap-2.5 rounded-full border bg-card px-4 py-2 text-sm shadow-md">
                            <span className="whitespace-nowrap text-muted-foreground">
                                {selectedCount} out of {totalTableCount} tables selected
                            </span>
                            {!disabled && (
                                <>
                                    <div className="h-4 w-px bg-border" />
                                    <button
                                        type="button"
                                        className="flex items-center gap-1.5 whitespace-nowrap text-foreground transition-colors hover:text-muted-foreground"
                                        onClick={() => onRowSelectionChange({})}
                                    >
                                        <XIcon className="size-3.5" aria-hidden="true" />
                                        Deselect all
                                    </button>
                                </>
                            )}
                        </div>
                    )
                }
            />
        </TooltipProvider>
    );
}
