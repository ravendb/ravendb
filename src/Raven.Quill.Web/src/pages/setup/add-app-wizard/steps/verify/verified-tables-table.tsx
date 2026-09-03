/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { useMemo } from "react";
import { getCoreRowModel, getFilteredRowModel, useReactTable, type RowSelectionState } from "@tanstack/react-table";
import { XIcon } from "lucide-react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { TooltipProvider } from "@/components/shadcn/ui/tooltip";
import { RANGE_PREVIEW_ROW_CLASSNAME, useRowRangeSelection } from "@/components/table/row-range-selection";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { getTableKey, MAX_SELECTED_TABLES } from "@/pages/setup/add-app-wizard/discover-utils";
import { VerifySchemaButton } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-button";
import { createVerifiedColumns } from "@/pages/setup/add-app-wizard/steps/verify/verify-schema-columns";

type VerifiedTablesTableProps = {
    tables: DiscoverTableResponse[];
    /** Total number of discovered tables, shown in the "x out of y" selection summary. */
    totalTableCount: number;
    search: string;
    rowSelection: RowSelectionState;
    onRowSelectionChange: (selection: RowSelectionState) => void;
    /** The selection is read-only: an imported configuration is locked, or a dry run is in flight. */
    disabled?: boolean;
    /** The wizard is running a step action, so the overlay must not start a second verification. */
    isBusy?: boolean;
};

export function VerifiedTablesTable({
    tables,
    totalTableCount,
    search,
    rowSelection,
    onRowSelectionChange,
    disabled,
    isBusy = false,
}: VerifiedTablesTableProps) {
    // react-table needs a stable filter-state reference between renders; a fresh identity on
    // every render makes it recompute row models and queue state resets, ending in a re-render
    // loop. "use no memo" opts this file out of the React Compiler (incompatible with react-table),
    // so the explicit useMemo matters. The `tables` data array is memoized by the parent.
    const columnFilters = useMemo(() => [{ id: "tableName", value: search }], [search]);
    const rangeSelection = useRowRangeSelection<DiscoverTableResponse>(MAX_SELECTED_TABLES);
    const columns = useMemo(
        () => createVerifiedColumns(rangeSelection.anchorRowIdRef),
        [rangeSelection.anchorRowIdRef],
    );

    const selectedCount = tables.filter((verifiedTable) => rowSelection[getTableKey(verifiedTable)]).length;

    const table = useReactTable({
        columns,
        data: tables,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: getTableKey,
        // At the limit only the already selected rows stay togglable, so the operator has to free a
        // slot before picking another table.
        enableRowSelection: (row) =>
            !disabled && (Boolean(rowSelection[row.id]) || selectedCount < MAX_SELECTED_TABLES),
        onRowSelectionChange: (updaterOrValue) => {
            const value = typeof updaterOrValue === "function" ? updaterOrValue(rowSelection) : updaterOrValue;
            onRowSelectionChange(value);
        },
        state: {
            rowSelection,
            columnFilters,
        },
    });

    // Shows the range a shift-click would take before it is taken, trimmed the same way the click is.
    const rangePreviewRowIds = rangeSelection.getPreviewRowIds(table);

    return (
        <TooltipProvider>
            <VirtualDataTable
                table={table}
                columnCount={columns.length}
                emptyMessage="No tables match the current filter."
                maxHeight="fill"
                getRowState={(rowId) => (rowSelection[rowId] ? "selected" : "")}
                getRowClassName={(rowId) => (rangePreviewRowIds.has(rowId) ? RANGE_PREVIEW_ROW_CLASSNAME : "")}
                onRowHoverChange={rangeSelection.onRowHoverChange}
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
                            <div className="h-4 w-px bg-border" />
                            <VerifySchemaButton disabled={isBusy} />
                        </div>
                    )
                }
            />
        </TooltipProvider>
    );
}
