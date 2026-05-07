import {
    useReactTable,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    ColumnDef,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import { Icon } from "components/common/Icon";
import { LoadError } from "components/common/LoadError";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import rootSqlTable from "models/database/tasks/sql/rootSqlTable";
import { useMemo } from "react";
import { UseAsyncReturn } from "react-async-hook";
import Button from "react-bootstrap/Button";
import { UseFieldArrayReturn } from "react-hook-form";

interface EditCdcSinkTaskDiscoveredTableProps {
    asyncFetchTables: UseAsyncReturn<rootSqlTable[], []>;
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskDiscoveredTable({
    asyncFetchTables,
    tablesFieldArray,
}: EditCdcSinkTaskDiscoveredTableProps) {
    const tablesData = useMemo(() => asyncFetchTables.result ?? [], [asyncFetchTables.result]);

    const table = useReactTable({
        data: tablesData,
        columns: tableColumnDefs,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    if (asyncFetchTables.status === "not-requested") {
        return (
            <div className="panel-bg-1 p-2 rounded border border-secondary hstack justify-content-center mt-1">
                <EmptySet compact>No tables have been discovered yet</EmptySet>
            </div>
        );
    }

    if (asyncFetchTables.status === "error") {
        return <LoadError error="Unable to load discovered tables" refresh={asyncFetchTables.execute} />;
    }

    const selectedRows = table.getSelectedRowModel().rows;
    const selectedCount = selectedRows.length;

    const handleAddSelected = () => {
        const getTableKey = (sourceName: string, sourceSchema: string) => {
            return `${sourceSchema}.${sourceName}`;
        };

        const existingKeys = new Set(
            tablesFieldArray.fields.map((f) => getTableKey(f.sourceTableName, f.sourceTableSchema))
        );

        const newTables = selectedRows
            .filter((r) => r.original && !existingKeys.has(getTableKey(r.original.tableName, r.original.tableSchema)))
            .map((r) => mapSqlTableToFormData(r.original));

        console.log("kalczur newTables", newTables);

        asyncFetchTables.status;

        newTables.forEach((newTable) => tablesFieldArray.append(newTable, { shouldFocus: false }));
        table.setRowSelection({});
    };

    return (
        <div className="position-relative">
            <VirtualTable table={table} heightInPx={300} isLoading={asyncFetchTables.loading} className="mt-2" />
            {selectedCount > 0 && (
                <div
                    className="position-absolute hstack gap-1 rounded-pill border border-secondary panel-bg-2 px-2"
                    style={{ bottom: "26px", left: "50%", transform: "translateX(-50%)" }}
                >
                    <span>
                        <b>{selectedCount}</b> selected
                    </span>
                    <div className="vr" />
                    <Button variant="link" onClick={handleAddSelected} className="text-reset p-0">
                        <Icon icon="plus" className="small" />
                        Configure selected tables
                    </Button>
                </div>
            )}
        </div>
    );
}

// TODO get percentage width
const tableColumnDefs: ColumnDef<rootSqlTable>[] = [
    columnCheckbox as ColumnDef<rootSqlTable>,
    {
        id: "TableName",
        header: "Table name",
        accessorFn: (x) => `${x.tableSchema}.${x.tableName}`,
        cell: CellValueWrapper,
        size: 300,
    },
    {
        id: "PrimaryKeys",
        header: "Primary keys",
        accessorFn: (x) => x.getPrimaryKeyColumnNames().join(", "),
        cell: CellValueWrapper,
        size: 200,
    },
    {
        id: "ColumnsCount",
        header: "Columns count",
        accessorFn: (x) => x.documentColumns().length,
        cell: CellValueWrapper,
        size: 120,
    },
];

type FormDataTable = NonNullable<EditCdcSinkTaskFormData["tables"]>[number];

function mapSqlTableToFormData(table: rootSqlTable): FormDataTable {
    const columns: NonNullable<FormDataTable["columns"]> = table.documentColumns().map((x) => ({
        column: x.sqlName,
        name: x.propertyName(),
        type: x.type === "Binary" ? "Attachment" : "Default",
    }));

    const primaryKeyColumns = table.getPrimaryKeyColumnNames();

    primaryKeyColumns.forEach((pk) => {
        const column = columns.find((c) => c.column === pk);
        if (!column) {
            columns.unshift({
                column: pk,
                name: pk,
                type: "Default",
            });
        }
    });

    return {
        collectionName: table.collectionName(),
        columns: columns,
        disabled: false,
        embeddedTables: [],
        linkedTables: table.getLinkedReferencesDto().map((x) => ({
            joinColumns: x.JoinColumns.map((value) => ({ value })),
            linkedCollectionName: x.Name,
            propertyName: x.Name,
            sourceTableName: x.SourceTableName,
            sourceTableSchema: x.SourceTableSchema,
        })),
        onDelete: { ignoreDeletes: false, patch: "" },
        patch: "",
        primaryKeyColumns: primaryKeyColumns.map((value) => ({ value })),
        sourceTableName: table.tableName,
        sourceTableSchema: table.tableSchema,
    } satisfies FormDataTable;
}
