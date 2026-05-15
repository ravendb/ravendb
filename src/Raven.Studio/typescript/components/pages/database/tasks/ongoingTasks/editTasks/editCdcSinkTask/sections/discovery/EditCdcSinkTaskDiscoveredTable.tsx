import {
    useReactTable,
    getCoreRowModel,
    getFilteredRowModel,
    getSortedRowModel,
    ColumnDef,
} from "@tanstack/react-table";
import { EmptySet } from "components/common/EmptySet";
import ExpandableList from "components/common/ExpandableList";
import { Icon } from "components/common/Icon";
import { LoadError } from "components/common/LoadError";
import RichAlert from "components/common/RichAlert";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { EditCdcSinkTaskFormData } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskValidation";
import { useCallback, useMemo, useState } from "react";
import { UseAsyncReturn } from "react-async-hook";
import Button from "react-bootstrap/Button";
import { UseFieldArrayReturn } from "react-hook-form";
import { upperFirst, camelCase } from "lodash";

import CdcSinkSchema = Raven.Client.Documents.Operations.CdcSink.Schema;
import {
    FormLinkedTable,
    FormRootTable,
    FormRootTableColumn,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/utils/editCdcSinkTaskTypes";

interface EditCdcSinkTaskDiscoveredTableProps {
    asyncGetSchema: UseAsyncReturn<CdcSinkSchema.CdcSinkSourceSchema, []>;
    tablesFieldArray: UseFieldArrayReturn<EditCdcSinkTaskFormData, "tables", "id">;
    widthPx: number;
}

export default function EditCdcSinkTaskDiscoveredTable({
    asyncGetSchema,
    tablesFieldArray,
    widthPx,
}: EditCdcSinkTaskDiscoveredTableProps) {
    const columns = useColumns(widthPx);
    const tablesData = useMemo(
        () => asyncGetSchema.result?.Tables.filter(isTableSupported) ?? [],
        [asyncGetSchema.result]
    );
    const unsupportedTables = useMemo(
        () => asyncGetSchema.result?.Tables.filter((table) => !isTableSupported(table)) ?? [],
        [asyncGetSchema.result]
    );

    const table = useReactTable({
        data: tablesData,
        columns,
        getCoreRowModel: getCoreRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        getSortedRowModel: getSortedRowModel(),
    });

    if (asyncGetSchema.status === "not-requested") {
        return (
            <div className="panel-bg-1 p-2 rounded border border-secondary hstack justify-content-center">
                <EmptySet compact>No tables have been discovered yet</EmptySet>
            </div>
        );
    }

    if (asyncGetSchema.status === "error") {
        return <LoadError error="Unable to discover tables" refresh={asyncGetSchema.execute} />;
    }

    const selectedRows = table.getSelectedRowModel().rows;
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
            .map((r) => mapSqlTableToFormData(r.original));

        newTables.forEach((newTable) => tablesFieldArray.append(newTable, { shouldFocus: false }));
        table.setRowSelection({});
    };

    return (
        <div className="position-relative">
            <SchemaAlerts errors={asyncGetSchema.result?.Errors ?? []} unsupportedTables={unsupportedTables} />
            <VirtualTable table={table} heightInPx={300} isLoading={asyncGetSchema.loading} />
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

interface SchemaAlertsProps {
    errors: string[];
    unsupportedTables: CdcSinkSchema.CdcSinkSourceTable[];
}

function SchemaAlerts({ errors, unsupportedTables }: SchemaAlertsProps) {
    return (
        <>
            {errors.length > 0 && (
                <RichAlert variant="danger" className="mb-2">
                    <AlertList items={errors} renderItem={(error) => error} />
                </RichAlert>
            )}
            {unsupportedTables.length > 0 && (
                <RichAlert variant="warning" className="mb-2">
                    <AlertList items={unsupportedTables} renderItem={getUnsupportedTableMessage} />
                </RichAlert>
            )}
        </>
    );
}

interface AlertListProps<T> {
    items: T[];
    renderItem: (item: T) => string;
}

function AlertList<T>({ items, renderItem }: AlertListProps<T>) {
    const [isExpanded, setIsExpanded] = useState(false);

    return (
        <ExpandableList
            itemsCount={items.length}
            collapsedItemsCount={2}
            isExpanded={isExpanded}
            setIsExpanded={setIsExpanded}
        >
            {({ visibleCount }) => (
                <div className="vstack gap-1">
                    {items.slice(0, visibleCount).map((item, index) => (
                        <div key={index}>{renderItem(item)}</div>
                    ))}
                </div>
            )}
        </ExpandableList>
    );
}

const useColumns = (widthPx: number): ColumnDef<CdcSinkSchema.CdcSinkSourceTable>[] => {
    const bodyWidth = virtualTableUtils.getTableBodyWidth(widthPx - columnCheckbox.size);
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
        ],
        [getSize]
    );
};

const pascalCase = (name: string) => upperFirst(camelCase(name));
const propertyNameFromJoinColumn = (name: string) => pascalCase(name.endsWith("_id") ? name.slice(0, -3) : name);

function isTableSupported(table: CdcSinkSchema.CdcSinkSourceTable) {
    return table.IsCdcEnabled && table.UnsupportedReason == null;
}

function getTableName(table: CdcSinkSchema.CdcSinkSourceTable) {
    return `${table.SourceTableSchema}.${table.SourceTableName}`;
}

function getUnsupportedTableMessage(table: CdcSinkSchema.CdcSinkSourceTable) {
    const reasons = [!table.IsCdcEnabled ? "CDC is not enabled" : null, table.UnsupportedReason].filter(Boolean);

    return `${getTableName(table)}: ${reasons.join(", ")}`;
}

function isColumnSupported(column: CdcSinkSchema.CdcSinkSourceColumn) {
    return column.IsCdcCapturable && column.UnsupportedReason == null;
}

function mapSqlTableToFormData(table: CdcSinkSchema.CdcSinkSourceTable): FormRootTable {
    const columns = table.Columns.filter(isColumnSupported).map(
        (x): FormRootTableColumn => ({
            column: x.Name,
            name: pascalCase(x.Name),
            type: x.SuggestedType,
        })
    );

    const linkedTables = table.ForeignKeys.map(
        (x): FormLinkedTable => ({
            propertyName: x.Columns.map(propertyNameFromJoinColumn).join("And"),
            joinColumns: x.Columns.map((value) => ({ value })),
            linkedCollectionName: pascalCase(x.ReferencedTable),
            sourceTableName: x.ReferencedTable,
            sourceTableSchema: x.ReferencedSchema,
        })
    );

    return {
        collectionName: pascalCase(table.SourceTableName),
        columns,
        disabled: false,
        embeddedTables: [],
        linkedTables,
        onDelete: { ignoreDeletes: false, patch: "" },
        patch: "",
        primaryKeyColumns: table.PrimaryKeyColumns.map((value) => ({ value })),
        sourceTableName: table.SourceTableName,
        sourceTableSchema: table.SourceTableSchema,
    } satisfies FormRootTable;
}
