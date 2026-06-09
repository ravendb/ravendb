/* eslint-disable react-hooks/incompatible-library */
"use no memo";

import { getCoreRowModel, getFilteredRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { Input } from "@/components/shadcn/ui/input";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { useFormContext } from "react-hook-form";
import { useState } from "react";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { Alert } from "@/components/shadcn/ui/alert";

export function VerifySchemaStep() {
    const { setValue, formState } = useFormContext<AppFormData>();
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);
    const [rowSelection, setRowSelection] = useState({});

    const allTables = discoverResult?.tables ?? [];

    const columns: ColumnDef<DiscoverTableResponse>[] = [
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
            size: 32,
        },
        {
            accessorFn: (table) => getTableLabel(table),
            header: "Table name",
            id: "tableName",
        },
        {
            accessorFn: (table) => table.primaryKeyColumns.join(", "),
            header: "Primary key",
            id: "primaryKey",
        },
        {
            accessorFn: (table) => table.columns.length,
            header: "Columns count",
            id: "columnsCount",
        },
    ];

    const table = useReactTable({
        columns,
        data: allTables,
        enableRowSelection: (row) => isTableUsable(row.original),
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        onRowSelectionChange: (updaterOrValue) => {
            const value = typeof updaterOrValue === "function" ? updaterOrValue(rowSelection) : updaterOrValue;
            setRowSelection(updaterOrValue);

            const ids = Object.keys(value);
            const selectedTables = table
                .getRowModel()
                .rows.filter((row) => ids.includes(row.id))
                .map((row) => row.original);

            setValue(
                "verifySchema.tables",
                selectedTables.map((table) => ({
                    sourceTableName: table.sourceTableName,
                    sourceTableSchema: table.sourceTableSchema,
                })),
                { shouldValidate: true },
            );
        },
        globalFilterFn: "includesString",
        state: {
            rowSelection,
        },
    });

    return (
        <div className="grid gap-3">
            <MessageList messages={discoverResult?.errors} tone="destructive" />
            <Input
                value={table.getColumn("tableName")?.getFilterValue() as string}
                onChange={(event) => table.getColumn("tableName")?.setFilterValue(event.target.value)}
                placeholder="Search by table name"
                className="max-w-sm"
                type="search"
            />
            <VirtualDataTable
                table={table}
                columnCount={columns.length}
                emptyMessage="No tables match the current filter."
                heightInPx={300}
            />
            {formState.errors?.verifySchema?.tables && (
                <Alert variant="destructive">{formState.errors.verifySchema.tables.message}</Alert>
            )}
        </div>
    );
}

export function MessageList({ messages, tone = "muted" }: { messages?: string[]; tone?: "destructive" | "muted" }) {
    const visibleMessages = messages?.filter(Boolean) ?? [];

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

function isTableUsable(table: DiscoverTableResponse) {
    return table.isCdcEnabled && !table.unsupportedReason;
}

function getTableLabel(table: DiscoverTableResponse) {
    return table.sourceTableSchema ? `${table.sourceTableSchema}.${table.sourceTableName}` : table.sourceTableName;
}
