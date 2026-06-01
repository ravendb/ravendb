/* eslint-disable react-refresh/only-export-components */
import { StepSection } from "@/pages/setup/add-app-wizard/wizard-step-section";
import type { WizardBodyComponentProps } from "@/components/form/wizard/form-wizard";
import { getCoreRowModel, getFilteredRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import type { DiscoverTableResponse } from "@/api/generated/server-api";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { Input } from "@/components/shadcn/ui/input";
import { cn } from "@/lib/utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/wizard-store";
import { Checkbox } from "@/components/shadcn/ui/checkbox";
import { useFieldArray, useFormContext } from "react-hook-form";
import { type AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";

export function VerifySchemaStep(props: WizardBodyComponentProps) {
    return (
        <StepSection {...props}>
            <SchemaTable />
        </StepSection>
    );
}

export function SchemaTable() {
    const { control } = useFormContext<AppFormData>();
    const discoverResult = useSetupWizardStore((state) => state.discoverResult);

    const tablesFieldArray = useFieldArray({
        control,
        name: "verifySchema.tables",
    });

    console.log("kalczur tablesFieldArray", tablesFieldArray.fields);

    const allTables = discoverResult?.tables ?? [];

    const columns: ColumnDef<DiscoverTableResponse>[] = [
        {
            id: "select",
            header: ({ table }) => (
                <Checkbox
                    checked={table.getIsAllRowsSelected()}
                    onChange={(event) => {
                        // TODO FIX
                        table.getToggleAllRowsSelectedHandler()(event);

                        if (event.currentTarget.value) {
                            tablesFieldArray.replace([]);
                        } else {
                            tablesFieldArray.replace(
                                allTables.map((x) => ({
                                    sourceTableName: x.sourceTableName,
                                    sourceTableSchema: x.sourceTableSchema,
                                })),
                            );
                        }
                    }}
                    aria-label="Select all"
                />
            ),
            cell: ({ row }) => (
                <Checkbox
                    checked={row.getIsSelected()}
                    onCheckedChange={(value) => {
                        row.toggleSelected();

                        // TODO simplify
                        if (value) {
                            tablesFieldArray.append({
                                sourceTableSchema: row.original.sourceTableSchema,
                                sourceTableName: row.original.sourceTableName,
                            });
                        } else {
                            const tableField = tablesFieldArray.fields.find(
                                (x) =>
                                    x.sourceTableSchema === row.original.sourceTableSchema &&
                                    x.sourceTableName === row.original.sourceTableName,
                            );

                            if (!tableField) {
                                return;
                            }

                            const fieldIndex = tablesFieldArray.fields.indexOf(tableField);

                            tablesFieldArray.remove(fieldIndex);
                        }
                    }}
                    aria-label="Select row"
                />
            ),
            enableSorting: false,
            size: 48,
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

    // eslint-disable-next-line react-hooks/incompatible-library
    const table = useReactTable({
        columns,
        data: allTables,
        enableRowSelection: (row) => isTableUsable(row.original),
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getRowId: (table) => getTableLabel(table),
        globalFilterFn: "includesString",
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
