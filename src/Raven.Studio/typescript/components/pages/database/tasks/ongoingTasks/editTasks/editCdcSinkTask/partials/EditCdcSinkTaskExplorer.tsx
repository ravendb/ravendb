import CollapseButton from "components/common/CollapseButton";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";
import { ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";

const tableColumnDefs: ColumnDef<SqlTableSchema>[] = [
    columnCheckbox as ColumnDef<SqlTableSchema>,
    {
        id: "TableName",
        header: "Table name",
        accessorFn: (x) => `${x.Schema}.${x.TableName}`,
        size: 200,
    },
    {
        id: "PrimaryKeys",
        header: "Primary keys",
        accessorFn: (x) => x.PrimaryKeyColumns.join(", "),
        size: 200,
    },
    {
        id: "ColumnsCount",
        header: "Columns count",
        accessorFn: (x) => x.Columns.length,
        size: 120,
    },
];

interface EditCdcSinkTaskExplorerProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

type CdcColumnType = Raven.Client.Documents.Operations.CdcSink.CdcColumnType;
type SqlColumnType = Raven.Server.SqlMigration.Schema.ColumnType;
type SqlTableSchema = Raven.Server.SqlMigration.Schema.SqlTableSchema;

function sqlColumnTypeToCdcType(type: SqlColumnType): CdcColumnType {
    switch (type) {
        case "Binary":
            return "Attachment";
        case "Array":
        case "Object":
            return "Json";
        default:
            return "Default";
    }
}

function mapSqlTableToFormData(t: SqlTableSchema): EditCdcSinkTaskFormData["tables"][number] {
    const pkSet = new Set(t.PrimaryKeyColumns);
    return {
        CollectionName: t.TableName,
        Columns: t.Columns.filter((c) => !pkSet.has(c.Name)).map((c) => ({
            Column: c.Name,
            Name: c.Name,
            Type: sqlColumnTypeToCdcType(c.Type),
        })),
        Disabled: false,
        EmbeddedTables: [],
        LinkedTables: [],
        OnDelete: { IgnoreDeletes: false, Patch: null },
        Patch: null,
        PrimaryKeyColumns: t.PrimaryKeyColumns,
        SourceTableName: t.TableName,
        SourceTableSchema: t.Schema ?? null,
    };
}

export default function EditCdcSinkTaskExplorer({ tablesFieldArray }: EditCdcSinkTaskExplorerProps) {
    const { value: isPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const { tasksService } = useServices();
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const [rowSelection, setRowSelection] = useState<Record<string, boolean>>({});

    // TODO get from connection string
    const asyncFetchTables = useAsyncCallback(() =>
        tasksService.fetchSqlDatabaseSchema(databaseName, {
            Provider: "NpgSQL",
            ConnectionString: "Host=localhost;Port=5432;Database=ravendb-net;Username=admin;Password=X5k@w9P!zFv7.JbQ",
            Schemas: null,
        })
    );

    const tables = asyncFetchTables.result?.Tables ?? [];

    const table = useReactTable({
        data: tables,
        columns: tableColumnDefs,
        state: { rowSelection },
        onRowSelectionChange: setRowSelection,
        getCoreRowModel: getCoreRowModel(),
        enableRowSelection: true,
    });

    const selectedCount = Object.keys(rowSelection).length;

    const handleAddSelected = () => {
        const existingKeys = new Set(tablesFieldArray.fields.map((f) => `${f.SourceTableSchema}.${f.SourceTableName}`));

        const newTables = table
            .getSelectedRowModel()
            .rows.map((r) => r.original)
            .filter((t) => !existingKeys.has(`${t.Schema}.${t.TableName}`))
            .map(mapSqlTableToFormData);

        if (newTables.length > 0) {
            tablesFieldArray.append(newTables);
        }

        setRowSelection({});
    };

    return (
        <div className="mt-3">
            <div className="hstack align-items-center">
                <h3 className="m-0">Schema Explorer</h3>
                <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
            </div>
            <div className="mb-1">Fetch existing tables from the linked source.</div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div>
                    <Button variant="secondary" onClick={asyncFetchTables.execute}>
                        Fetch tables
                    </Button>
                    {selectedCount > 0 && (
                        <Button variant="primary" className="ms-2" onClick={handleAddSelected}>
                            Add selected ({selectedCount})
                        </Button>
                    )}
                    {(asyncFetchTables.loading || asyncFetchTables.result) && (
                        <VirtualTable
                            table={table}
                            heightInPx={300}
                            isLoading={asyncFetchTables.loading}
                            className="mt-2"
                        />
                    )}
                </div>
            </Collapse>
        </div>
    );
}
