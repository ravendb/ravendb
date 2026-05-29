import { getCoreRowModel, useReactTable, type ColumnDef } from "@tanstack/react-table";
import type { CdcSinkConfiguration } from "@/api/generated/server-api";
import { VirtualDataTable } from "@/components/table/virtual-data-table";
import { getMappedTableKey } from "@/pages/setup/add-app-wizard/wizard-model";

type MappedTable = NonNullable<CdcSinkConfiguration["tables"]>[number];

export function MappingTable({ configuration }: { configuration: CdcSinkConfiguration | null }) {
    const tables = configuration?.tables ?? [];
    const columns: ColumnDef<MappedTable>[] = [
        {
            accessorFn: (table) => table.collectionName ?? "",
            header: "Collection",
            id: "collection",
        },
        {
            accessorFn: (table) => getMappedTableKey(table),
            header: "Source table",
            id: "sourceTable",
        },
        {
            accessorFn: (table) => table.columns?.length ?? 0,
            header: "Fields",
            id: "fields",
        },
    ];

    // eslint-disable-next-line react-hooks/incompatible-library
    const table = useReactTable({
        columns,
        data: tables,
        getCoreRowModel: getCoreRowModel(),
        getRowId: (table) => getMappedTableKey(table),
    });

    if (!configuration) {
        return (
            <div className="rounded-lg border bg-background px-3 py-8 text-center text-sm text-muted-foreground">
                No mapping generated yet.
            </div>
        );
    }

    return (
        <VirtualDataTable
            table={table}
            columnCount={columns.length}
            emptyMessage="No mapped tables."
            getCellClassName={(cellId) => (cellId.endsWith("_collection") ? undefined : "text-muted-foreground")}
        />
    );
}
