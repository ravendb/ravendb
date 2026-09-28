import { ColumnDef, Table as TanstackTable, VisibilityState } from "@tanstack/react-table";
import changeVectorUtils from "common/changeVectorUtils";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { AppliedColumnLayout } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import {
    createCustomColumnAccessor,
    CustomColumnDefinition,
    getCustomColumnProperties,
} from "components/common/virtualTable/commonComponents/columnsSelect/customColumns";
import { columnCheckbox, createLazySelectionColumn } from "components/common/virtualTable/utils/commonColumnDefs";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useAppUrls } from "components/hooks/useAppUrls";
import { FullDocumentProvider } from "components/pages/database/documents/documentsList/hooks/useFullDocumentProvider";
import { documentsColumnLayoutStorage } from "components/pages/database/documents/documentsList/utils/documentsColumnLayoutStorage";
import { sumBy, uniq } from "lodash";
import document from "models/database/documents/document";
import { useMemo, useState } from "react";

interface UseDocumentsColumnsProps {
    databaseName: string;
    collectionName: string | null;
    tableBodyWidthInPx: number;
    getPropertyPreviewResolver: FullDocumentProvider["getPropertyPreviewResolver"];
}

const noColumns: string[] = [];
const noCustomColumns: CustomColumnDefinition[] = [];
const flagsColumnWidth = 130;
const propertyColumnWidth = 150;
const customColumnWidth = 200;
const metadataColumnName = "__metadata";
const idColumnName = "@id";
const changeVectorColumnId = "Change Vector";
const lastModifiedColumnId = "Last Modified";
const collectionColumnId = "Collection";

const selectionColumn = createLazySelectionColumn<document>("Select all documents", "Select document");

const flagsColumn: ColumnDef<document> = { ...columnDocumentFlags, size: flagsColumnWidth };

type AppUrl = ReturnType<typeof useAppUrls>["appUrl"];

export function useDocumentsColumns({
    databaseName,
    collectionName,
    tableBodyWidthInPx,
    getPropertyPreviewResolver,
}: UseDocumentsColumnsProps) {
    const { appUrl } = useAppUrls();
    const isAllDocuments = collectionName === null;
    const propertyColumnsWidthInPx = tableBodyWidthInPx - columnCheckbox.size - flagsColumnWidth;

    const [appliedLayout, setAppliedLayout] = useState<AppliedColumnLayout | null>(() =>
        isAllDocuments ? null : documentsColumnLayoutStorage.load(databaseName, collectionName)
    );
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);
    const [previewedColumns, setPreviewedColumns] = useState<string[]>([]);

    const customColumns = appliedLayout?.customColumns ?? noCustomColumns;

    const fullBindings = useMemo(() => (appliedLayout ? getFullBindings(appliedLayout) : noColumns), [appliedLayout]);

    const previewBindings = useMemo(
        () =>
            appliedLayout
                ? getPreviewBindings(appliedLayout, isAllDocuments).filter((x) => !fullBindings.includes(x))
                : noColumns,
        [appliedLayout, isAllDocuments, fullBindings]
    );

    const propertyColumnNames = useMemo(
        () => (isAllDocuments ? availableColumns.filter((x) => x !== metadataColumnName) : availableColumns),
        [isAllDocuments, availableColumns]
    );

    const propertyColumns = useDocumentColumnsProvider({
        columnNames: propertyColumnNames,
        availableWidth: propertyColumnsWidthInPx,
        columnsWithValues: isAllDocuments ? noColumns : previewedColumns,
        databaseName,
        getPreviewValueResolver: getPropertyPreviewResolver,
    });

    const customColumnDefs = useMemo(
        () => customColumns.map((column) => createCustomColumn(column, databaseName)),
        [customColumns, databaseName]
    );

    const columnDefs = useMemo(
        () => [
            selectionColumn,
            ...(isAllDocuments ? createMetadataColumns(databaseName, propertyColumnsWidthInPx, appUrl) : []),
            ...propertyColumns.columnDefs,
            ...customColumnDefs,
            flagsColumn,
        ],
        [isAllDocuments, databaseName, propertyColumnsWidthInPx, appUrl, propertyColumns.columnDefs, customColumnDefs]
    );

    const tableState = useMemo(
        () => ({
            columnVisibility: appliedLayout
                ? getLayoutVisibility(columnDefs, appliedLayout)
                : propertyColumns.initialColumnVisibility,
            columnOrder: appliedLayout?.columnOrder ?? noColumns,
            columnPinning: { left: [columnCheckbox.id, ...(appliedLayout?.pinnedColumnIds ?? noColumns)] },
        }),
        [appliedLayout, columnDefs, propertyColumns.initialColumnVisibility]
    );

    const fittedColumnDefs = useMemo(
        () => fitVisibleColumnsToWidth(columnDefs, tableState.columnVisibility, tableBodyWidthInPx),
        [columnDefs, tableState.columnVisibility, tableBodyWidthInPx]
    );

    const defaultVisibleColumnIds = columnDefs
        .map((column) => column.id)
        .filter((id) => propertyColumns.initialColumnVisibility[id] !== false);

    return {
        columnDefs: fittedColumnDefs,
        tableState,
        previewBindings,
        fullBindings,
        onPreviewResult: (result: pagedResultWithAvailableColumns<document>) => {
            setAvailableColumns((prev) => mergeColumnNames(prev, result.availableColumns));
            setPreviewedColumns((prev) => mergeColumnNames(prev, uniq(result.items.flatMap((x) => Object.keys(x)))));
        },
        isCustomLayout: appliedLayout !== null,
        settingsOptions: {
            customColumns,
            onApplied: (layout: AppliedColumnLayout) => {
                if (!isAllDocuments) {
                    documentsColumnLayoutStorage.save(databaseName, collectionName, layout);
                }
                setAppliedLayout(layout);
            },
            restoreDefaults: {
                visibleColumnIds: defaultVisibleColumnIds,
                onRestore: () => {
                    documentsColumnLayoutStorage.clear(databaseName, collectionName);
                    setAppliedLayout(null);
                },
            },
        },
        getExportFields: (table: TanstackTable<document>) =>
            table
                .getVisibleLeafColumns()
                .map((column) => column.id)
                .filter((id) => id === idColumnName || availableColumns.includes(id)),
    };
}

function getPreviewBindings(layout: AppliedColumnLayout, isAllDocuments: boolean): string[] {
    const nonPropertyColumnIds = new Set<string>([
        columnCheckbox.id,
        idColumnName,
        columnDocumentFlags.id,
        ...(isAllDocuments ? [changeVectorColumnId, lastModifiedColumnId, collectionColumnId] : []),
        ...layout.customColumns.map((column) => column.id),
    ]);

    return layout.visibleColumnIds.filter((id) => !nonPropertyColumnIds.has(id));
}

function getFullBindings(layout: AppliedColumnLayout): string[] {
    const visibleCustomColumns = layout.customColumns.filter((column) => layout.visibleColumnIds.includes(column.id));

    return uniq(visibleCustomColumns.flatMap((column) => getCustomColumnProperties(column.expression)));
}

function fitVisibleColumnsToWidth(
    columnDefs: ColumnDef<document>[],
    columnVisibility: VisibilityState,
    availableWidth: number
): ColumnDef<document>[] {
    const getSize = (column: ColumnDef<document>) => column.size ?? propertyColumnWidth;
    const isFixed = (column: ColumnDef<document>) =>
        column.id === columnCheckbox.id || column.id === columnDocumentFlags.id;

    const visibleColumns = columnDefs.filter((column) => columnVisibility[column.id] !== false);
    const stretchedColumns = visibleColumns.filter((column) => !isFixed(column));

    if (stretchedColumns.length === 0) {
        return columnDefs;
    }

    const fixedWidth = sumBy(visibleColumns.filter(isFixed), getSize);
    const scale = (availableWidth - fixedWidth) / sumBy(stretchedColumns, getSize);

    return columnDefs.map((column) =>
        stretchedColumns.includes(column)
            ? { ...column, size: Math.max(propertyColumnWidth, Math.floor(getSize(column) * scale)) }
            : column
    );
}

function getLayoutVisibility(columnDefs: ColumnDef<document>[], layout: AppliedColumnLayout): VisibilityState {
    return Object.fromEntries(
        columnDefs
            .filter((column) => column.enableHiding !== false)
            .map((column) => [column.id, layout.visibleColumnIds.includes(column.id)])
    );
}

function mergeColumnNames(previous: string[], next: string[]): string[] {
    const addedColumnNames = next.filter((name) => !previous.includes(name));

    return addedColumnNames.length === 0 ? previous : [...previous, ...addedColumnNames];
}

function createCustomColumn(column: CustomColumnDefinition, databaseName: string): ColumnDef<document> {
    const getValue = createCustomColumnAccessor(column.expression);

    return {
        id: column.id,
        header: column.header,
        accessorFn: (doc) => getValue(doc),
        cell: ({ getValue }) => <CellDocumentValue value={getValue()} databaseName={databaseName} hasHyperlinkForIds />,
        size: customColumnWidth,
        meta: { customColumn: column },
    };
}

function createMetadataColumns(databaseName: string, widthInPx: number, appUrl: AppUrl): ColumnDef<document>[] {
    const getSize = virtualTableUtils.getCellSizeProvider(widthInPx);

    return [
        {
            id: idColumnName,
            header: "Id",
            accessorFn: (doc) => doc.getId(),
            cell: ({ getValue }) => {
                const id = getValue<string>();

                return (
                    <CellWithCopy value={id}>
                        <a href={appUrl.forEditDoc(id, databaseName)}>{id}</a>
                    </CellWithCopy>
                );
            },
            size: getSize(30),
            enableHiding: false,
        },
        {
            id: changeVectorColumnId,
            header: "Change Vector",
            accessorFn: (doc) => doc.__metadata.changeVector(),
            cell: ({ getValue }) => {
                const changeVector = getValue<string>();

                return (
                    <CellWithCopy value={changeVector}>
                        <CellValue
                            value={changeVector ? changeVectorUtils.formatChangeVectorAsShortString(changeVector) : ""}
                        />
                    </CellWithCopy>
                );
            },
            size: getSize(20),
        },
        {
            id: lastModifiedColumnId,
            header: "Last Modified",
            accessorFn: (doc) => doc.__metadata.lastModified(),
            cell: DateFormatterCell,
            size: getSize(25),
        },
        {
            id: collectionColumnId,
            header: "Collection",
            accessorFn: (doc) => doc.getCollection(),
            cell: ({ getValue }) => {
                const collection = getValue<string>();

                return (
                    <CellWithCopy value={collection}>
                        <a href={appUrl.forDocuments(collection, databaseName)}>{collection}</a>
                    </CellWithCopy>
                );
            },
            size: getSize(25),
        },
    ];
}
