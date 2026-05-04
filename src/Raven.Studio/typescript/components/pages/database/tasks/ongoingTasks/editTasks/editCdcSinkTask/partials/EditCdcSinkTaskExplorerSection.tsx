import CollapseButton from "components/common/CollapseButton";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import useBoolean from "components/hooks/useBoolean";
import { useServices } from "components/hooks/useServices";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useAppSelector } from "components/store";
import { useMemo, useState } from "react";
import { useAsyncCallback } from "react-async-hook";
import Button from "react-bootstrap/Button";
import Collapse from "react-bootstrap/esm/Collapse";
import { UseFieldArrayReturn } from "react-hook-form";
import { ColumnDef, getCoreRowModel, getFilteredRowModel, useReactTable } from "@tanstack/react-table";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import sqlMigration from "models/database/tasks/sql/sqlMigration";
import rootSqlTable from "models/database/tasks/sql/rootSqlTable";
import { editCdcSinkTaskSelectors } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import assertUnreachable from "components/utils/assertUnreachable";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { Icon } from "components/common/Icon";
import { EmptySet } from "components/common/EmptySet";

interface EditCdcSinkTaskExplorerSectionProps {
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
}

export default function EditCdcSinkTaskExplorerSection({ tablesFieldArray }: EditCdcSinkTaskExplorerSectionProps) {
    const { tasksService } = useServices();
    const { value: isPanelOpen, toggle: toggleIsPanelOpen } = useBoolean(true);
    const [rowSelection, setRowSelection] = useState<Record<string, boolean>>({});
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const connectionString = useAppSelector(editCdcSinkTaskSelectors.selectedConnectionString);

    const asyncFetchTables = useAsyncCallback(async () => {
        const provider = getProviderFromFactoryName(connectionString.FactoryName);

        const result = await tasksService.fetchSqlDatabaseSchema(databaseName, {
            Provider: provider,
            ConnectionString: connectionString.ConnectionString,
            Schemas: null,
        });

        const model = new sqlMigration();
        model.onSchemaUpdated(result);

        console.log("kalczur model dto", model.toDto());

        return model.tables();
    });

    const tables = useMemo(() => asyncFetchTables.result ?? [], [asyncFetchTables.result]);

    const table = useReactTable({
        data: tables,
        columns: tableColumnDefs,
        state: { rowSelection },
        onRowSelectionChange: setRowSelection,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getCoreRowModel(),
        enableRowSelection: true,
    });

    const selectedCount = Object.keys(rowSelection).length;

    const handleAddSelected = () => {
        const getTableKey = (sourceName: string, sourceSchema: string) => {
            return `${sourceSchema}.${sourceName}`;
        };

        const existingKeys = new Set(
            tablesFieldArray.fields.map((f) => getTableKey(f.SourceTableName, f.SourceTableSchema))
        );

        const newTables = table
            .getSelectedRowModel()
            .rows.filter((r) => !existingKeys.has(getTableKey(r.original.tableName, r.original.tableSchema)))
            .map((r) => mapSqlTableToFormData(r.original));

        console.log("kalczur newTables", newTables);

        if (newTables.length > 0) {
            tablesFieldArray.append(newTables);
        }

        setRowSelection({});
    };

    return (
        <div className="mt-3">
            <div className="hstack justify-content-between align-items-end">
                <div>
                    <div className="hstack align-items-center">
                        <h3 className="m-0">Schema Explorer</h3>
                        <CollapseButton isExpanded={isPanelOpen} toggle={toggleIsPanelOpen} />
                    </div>
                    <div className="mb-1">Fetch existing tables from the linked source.</div>
                </div>
                <ConditionalPopover
                    conditions={{
                        isActive: !connectionString,
                        message: "Please provide a connection string to fetch tables.",
                    }}
                >
                    <Button
                        variant="secondary"
                        className="rounded-pill"
                        onClick={asyncFetchTables.execute}
                        disabled={!connectionString}
                    >
                        <Icon icon="search" />
                        Discover tables
                    </Button>
                </ConditionalPopover>
            </div>
            <Collapse in={isPanelOpen} mountOnEnter unmountOnExit>
                <div className="position-relative">
                    {!asyncFetchTables.result && !asyncFetchTables.loading && (
                        <div className="panel-bg-1 p-2 rounded border border-secondary hstack justify-content-center mt-1">
                            <EmptySet compact>No tables have been discovered yet</EmptySet>
                        </div>
                    )}
                    {(asyncFetchTables.loading || asyncFetchTables.result) && (
                        <VirtualTable
                            table={table}
                            heightInPx={300}
                            isLoading={asyncFetchTables.loading}
                            className="mt-2"
                        />
                    )}
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
            </Collapse>
        </div>
    );
}

function getProviderFromFactoryName(
    factoryName: SqlConnectionStringFactoryName
): Raven.Server.SqlMigration.MigrationProvider {
    switch (factoryName) {
        case "Microsoft.Data.SqlClient":
        case "System.Data.SqlClient":
            return "MsSQL";
        case "MySqlConnector.MySqlConnectorFactory":
            return "MySQL_MySqlConnector";
        case "MySql.Data.MySqlClient":
            return "MySQL_MySql_Data";
        case "Npgsql":
            return "NpgSQL";
        case "Oracle.ManagedDataAccess.Client":
            return "Oracle";
        default:
            assertUnreachable(factoryName);
    }
}

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

type FormDataTable = EditCdcSinkTaskFormData["tables"][number];

function mapSqlTableToFormData(table: rootSqlTable): FormDataTable {
    const columns: FormDataTable["Columns"] = table.documentColumns().map((x) => ({
        Column: x.sqlName,
        Name: x.propertyName(),
        Type: x.type === "Binary" ? "Attachment" : "Default",
    }));

    const primaryKeyColumns = table.getPrimaryKeyColumnNames();

    primaryKeyColumns.forEach((pk) => {
        const column = columns.find((c) => c.Column === pk);
        if (!column) {
            columns.unshift({
                Column: pk,
                Name: pk,
                Type: "Default",
            });
        }
    });

    return {
        CollectionName: table.collectionName(),
        Columns: columns,
        Disabled: false,
        EmbeddedTables: [], // TODO maybe? create or or leave empty
        LinkedTables: table.getLinkedReferencesDto().map((x) => ({
            JoinColumns: x.JoinColumns,
            LinkedCollectionName: x.Name,
            PropertyName: x.Name,
            SourceTableName: x.SourceTableName,
            SourceTableSchema: x.SourceTableSchema,
        })),
        OnDelete: { IgnoreDeletes: false, Patch: null },
        Patch: null,
        PrimaryKeyColumns: primaryKeyColumns,
        SourceTableName: table.tableName,
        SourceTableSchema: table.tableSchema,
    } satisfies FormDataTable;
}
