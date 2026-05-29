import {
    getCoreRowModel,
    getFilteredRowModel,
    useReactTable,
    type ColumnDef,
    type RowSelectionState,
} from "@tanstack/react-table";
import { useState } from "react";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { Input } from "@/components/shadcn/ui/input";
import { cn } from "@/lib/utils";
import {
    getPrimaryKeyLabel,
    getTableKey,
    getTableLabel,
    isTableUsable,
} from "@/pages/setup/add-app-wizard/wizard-model";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";

export function SchemaTable() {
    const schema = useSetupWizardStore((state) => state.schema);
    const selectedTableKeys = useSetupWizardStore((state) => state.selectedTableKeys);
    const selectTableKeys = useSetupWizardStore((state) => state.selectTableKeys);
    const [globalFilter, setGlobalFilter] = useState("");

    const schemaTables = schema?.tables ?? [];
    const rowSelection: RowSelectionState = Object.fromEntries(selectedTableKeys.map((tableKey) => [tableKey, true]));
    const columns: ColumnDef<DiscoverTableResponse>[] = [
        {
            id: "select",
            header: ({ table }) => {
                const selectableRows = table.getFilteredRowModel().rows.filter((row) => row.getCanSelect());
                const selectedRows = selectableRows.filter((row) => row.getIsSelected());
                const isAllSelected = selectableRows.length > 0 && selectedRows.length === selectableRows.length;
                const isPartiallySelected = selectedRows.length > 0 && !isAllSelected;

                return (
                    <input
                        type="checkbox"
                        aria-label="Select all visible verified tables"
                        checked={isAllSelected}
                        ref={(input) => {
                            if (input) {
                                input.indeterminate = isPartiallySelected;
                            }
                        }}
                        onChange={(event) => {
                            const nextSelectedTableKeys = new Set(selectedTableKeys);

                            for (const row of selectableRows) {
                                if (event.target.checked) {
                                    nextSelectedTableKeys.add(row.id);
                                } else {
                                    nextSelectedTableKeys.delete(row.id);
                                }
                            }

                            selectTableKeys([...nextSelectedTableKeys]);
                        }}
                        className="size-4 rounded border-border accent-primary"
                    />
                );
            },
            cell: ({ row }) => (
                <input
                    type="checkbox"
                    aria-label={`Use ${getTableLabel(row.original)}`}
                    checked={row.getIsSelected()}
                    disabled={!row.getCanSelect()}
                    onChange={row.getToggleSelectedHandler()}
                    className="size-4 rounded border-border accent-primary disabled:cursor-not-allowed disabled:opacity-50"
                />
            ),
            size: 48,
        },
        {
            accessorFn: (table) => getTableLabel(table),
            header: "Table name",
            id: "tableName",
        },
        {
            accessorFn: (table) => getPrimaryKeyLabel(table),
            header: "Primary key",
            id: "primaryKey",
        },
        {
            accessorFn: (table) => table.columns.length,
            header: "Columns count",
            id: "columnsCount",
        },
        {
            accessorFn: (table) => (isTableUsable(table) ? "Ready" : table.unsupportedReason || "Unsupported"),
            header: "Status",
            id: "status",
        },
    ];

    // eslint-disable-next-line react-hooks/incompatible-library
    const table = useReactTable({
        columns,
        data: schemaTables,
        enableRowSelection: (row) => isTableUsable(row.original),
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (table) => getTableKey(table),
        globalFilterFn: "includesString",
        onGlobalFilterChange: setGlobalFilter,
        onRowSelectionChange: (updater) => {
            const nextRowSelection = typeof updater === "function" ? updater(rowSelection) : updater;

            selectTableKeys(
                Object.entries(nextRowSelection).flatMap(([tableKey, isSelected]) => (isSelected ? [tableKey] : [])),
            );
        },
        state: {
            globalFilter,
            rowSelection,
        },
    });

    if (!schema) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                Tables will appear here after the source is verified.
            </div>
        );
    }

    return (
        <div className="grid gap-3">
            <MessageList messages={schema.errors} tone="destructive" />
            <Input
                value={globalFilter}
                onChange={(event) => setGlobalFilter(event.target.value)}
                placeholder="Filter tables..."
                className="max-w-sm"
            />
            <VirtualDataTable
                table={table}
                columnCount={columns.length}
                emptyMessage="No tables match the current filter."
                heightInPx={300}
                getCellClassName={(cellId) =>
                    cellId.endsWith("_tableName") ? "text-foreground" : "text-muted-foreground"
                }
                getRowState={(rowId) => (rowSelection[rowId] ? "selected" : undefined)}
            />
        </div>
    );
}

export function MessageList({ messages, tone = "muted" }: { messages: string[]; tone?: "destructive" | "muted" }) {
    const visibleMessages = messages.filter(Boolean);

    if (visibleMessages.length === 0) {
        return null;
    }

    return (
        <ul className={cn("grid gap-1 text-sm", tone === "destructive" ? "text-destructive" : "text-muted-foreground")}>
            {visibleMessages.map((message, index) => (
                <li key={index}>{message}</li>
            ))}
        </ul>
    );
}
