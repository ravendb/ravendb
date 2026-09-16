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
import RichAlert from "components/common/RichAlert";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useCallback, useMemo } from "react";
import { UseAsyncReturn } from "react-async-hook";
import Button from "react-bootstrap/Button";
import { UseFieldArrayReturn } from "react-hook-form";

import CdcSinkSchema = Raven.Client.Documents.Operations.CdcSink.Schema;
import ExpandableListContainer from "components/common/ExpandableListContainer";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import {
    isTableSupported,
    mapSqlTableToFormData,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskSchemaUtils";

interface EditCdcSinkTaskDiscoveredTableProps {
    asyncGetSchema: UseAsyncReturn<CdcSinkSchema.CdcSinkSourceSchema, [string[]]>;
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
    widthPx: number;
}

export default function EditCdcSinkTaskDiscoveredTable({
    asyncGetSchema,
    tablesFieldArray,
    widthPx,
}: EditCdcSinkTaskDiscoveredTableProps) {
    const selectableColumns = useSelectableColumns(widthPx);
    const unavailableColumns = useUnavailableColumns(widthPx);
    const sourceSchema = asyncGetSchema.result;
    const selectableTables = useMemo(
        () => sourceSchema?.Tables.filter((table) => isTableSupported(sourceSchema, table)) ?? [],
        [sourceSchema]
    );
    const unavailableTables = useMemo(
        () =>
            sourceSchema?.Tables.filter((table) => sourceSchema.Success && !isTableSupported(sourceSchema, table)) ??
            [],
        [sourceSchema]
    );

    const selectableTablesTable = useReactTable({
        data: selectableTables,
        columns: selectableColumns,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
        initialState: {
            sorting: [
                {
                    id: "TableName",
                    desc: true,
                },
            ],
        },
    });

    const unavailableTablesTable = useReactTable({
        data: unavailableTables,
        columns: unavailableColumns,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
        initialState: {
            sorting: [
                {
                    id: "TableName",
                    desc: true,
                },
            ],
        },
    });

    if (asyncGetSchema.status === "not-requested") {
        return (
            <div className="panel-bg-1 p-2 rounded border border-secondary hstack justify-content-center">
                <EmptySet compact className="text-muted">
                    Click &quot;Discover tables&quot; to fetch available tables from the source database.
                </EmptySet>
            </div>
        );
    }

    if (asyncGetSchema.status === "error") {
        return <LoadError error="Unable to discover tables" />;
    }

    const selectedRows = selectableTablesTable.getSelectedRowModel().rows;
    const selectedCount = selectedRows.length;

    const handleAddSelected = () => {
        const getTableKey = (sourceSchema: string, sourceName: string) => {
            return `${sourceSchema}.${sourceName}`;
        };

        const existingKeys = new Set(
            tablesFieldArray.fields.map((f) => getTableKey(f.sourceTableSchema, f.sourceTableName))
        );

        const newTables = selectedRows
            .filter(
                (r) =>
                    r.original &&
                    !existingKeys.has(getTableKey(r.original.SourceTableSchema, r.original.SourceTableName))
            )
            .map((r) => mapSqlTableToFormData(sourceSchema, r.original));

        newTables.forEach((newTable) => tablesFieldArray.append(newTable, { shouldFocus: false }));
        selectableTablesTable.setRowSelection({});
    };

    return (
        <div>
            <SchemaAlerts schema={sourceSchema} />
            <div className="vstack gap-2">
                <div>
                    <h4 className="mb-1">Available tables</h4>
                    <div className="position-relative">
                        <VirtualTable
                            table={selectableTablesTable}
                            heightInPx={virtualTableUtils.getHeightInPx(selectableTables.length, 300)}
                            isLoading={asyncGetSchema.loading}
                        />
                        {selectedCount > 0 && (
                            <div
                                className="position-absolute hstack gap-1 rounded-pill border border-secondary panel-bg-3 px-2"
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
                </div>
                {unavailableTables.length > 0 && (
                    <div className="mt-2">
                        <h4 className="mb-1">
                            <Icon icon="warning" color="warning" />
                            Unavailable tables
                        </h4>
                        <VirtualTable
                            table={unavailableTablesTable}
                            heightInPx={virtualTableUtils.getHeightInPx(unavailableTables.length, 300)}
                        />
                    </div>
                )}
            </div>
        </div>
    );
}

interface SchemaAlertsProps {
    schema: CdcSinkSchema.CdcSinkSourceSchema;
}

function SchemaAlerts({ schema }: SchemaAlertsProps) {
    return (
        <>
            {schema?.Errors.length > 0 && (
                <RichAlert variant="danger" className="mb-2">
                    <ExpandableListContainer items={schema.Errors} renderItem={(err) => err} />
                </RichAlert>
            )}
            {schema?.Warnings.length > 0 && (
                <RichAlert variant="warning" className="mb-2">
                    <ExpandableListContainer items={schema.Warnings} renderItem={(warning) => warning} />
                </RichAlert>
            )}
        </>
    );
}

const useSelectableColumns = (widthPx: number): ColumnDef<CdcSinkSchema.CdcSinkSourceTable>[] => {
    const warningsColumnWidth = 100;
    const bodyWidth = virtualTableUtils.getTableBodyWidth(widthPx - columnCheckbox.size - warningsColumnWidth);
    const getSize = useCallback(virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    return useMemo<ColumnDef<CdcSinkSchema.CdcSinkSourceTable>[]>(
        () => [
            columnCheckbox as ColumnDef<CdcSinkSchema.CdcSinkSourceTable>,
            {
                id: "TableName",
                header: "Table name",
                accessorFn: getTableName,
                cell: CellValueWrapper,
                size: getSize(50),
            },
            {
                id: "PrimaryKeys",
                header: "Primary keys",
                accessorFn: (x) => x.PrimaryKeyColumns.join(", "),
                cell: CellValueWrapper,
                size: getSize(30),
            },
            {
                id: "ColumnsCount",
                header: "Columns count",
                accessorFn: (x) => x.Columns.length,
                cell: CellValueWrapper,
                size: getSize(20),
            },
            {
                id: "Warnings",
                header: "Warnings",
                accessorFn: (x) => x.Warnings,
                cell: ({ getValue }) => {
                    const warnings = getValue<string[]>();
                    if (!warnings?.length) {
                        return null;
                    }

                    return (
                        <PopoverWithHoverWrapper
                            message={<ExpandableListContainer items={warnings} renderItem={(warning) => warning} />}
                        >
                            <Icon icon="warning" color="warning" margin="m-0" aria-label="Table warnings" />
                        </PopoverWithHoverWrapper>
                    );
                },
                size: warningsColumnWidth,
                enableSorting: false,
                enableFiltering: false,
                enableColumnFilter: false,
            },
        ],
        [getSize]
    );
};

const useUnavailableColumns = (widthPx: number): ColumnDef<CdcSinkSchema.CdcSinkSourceTable>[] => {
    const errorColumnWidth = 100;
    const bodyWidth = virtualTableUtils.getTableBodyWidth(widthPx - errorColumnWidth);
    const getSize = useCallback(virtualTableUtils.getCellSizeProvider(bodyWidth), [bodyWidth]);

    return useMemo<ColumnDef<CdcSinkSchema.CdcSinkSourceTable>[]>(
        () => [
            {
                id: "TableName",
                header: "Table name",
                accessorFn: getTableName,
                cell: CellValueWrapper,
                size: getSize(50),
            },
            {
                id: "PrimaryKeys",
                header: "Primary keys",
                accessorFn: (x) => x.PrimaryKeyColumns.join(", "),
                cell: CellValueWrapper,
                size: getSize(30),
            },
            {
                id: "ColumnsCount",
                header: "Columns count",
                accessorFn: (x) => x.Columns.length,
                cell: CellValueWrapper,
                size: getSize(20),
            },
            {
                id: "Error",
                header: "Error",
                accessorFn: getUnavailableTableMessage,
                cell: ({ getValue }) => (
                    <PopoverWithHoverWrapper message={getValue<string>()}>
                        <Icon icon="danger" color="danger" margin="m-0" aria-label="CDC setup required" />
                    </PopoverWithHoverWrapper>
                ),
                size: errorColumnWidth,
                enableSorting: false,
                enableFiltering: false,
                enableColumnFilter: false,
            },
        ],
        [getSize]
    );
};

function getTableName(table: CdcSinkSchema.CdcSinkSourceTable) {
    return `${table.SourceTableSchema}.${table.SourceTableName}`;
}

function getUnavailableTableMessage(table: CdcSinkSchema.CdcSinkSourceTable) {
    if (table.UnsupportedReason) {
        return table.UnsupportedReason;
    }

    if (!table.IsCdcEnabled) {
        return "CDC is not enabled. Ask a database administrator to enable CDC for this table.";
    }

    return "This table cannot be configured for CDC.";
}
