import { ColumnDef, Table as TanstackTable, VisibilityState } from "@tanstack/react-table";
import changeVectorUtils from "common/changeVectorUtils";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import CellDocumentId from "components/common/virtualTable/cells/CellDocumentId";
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

const NO_COLUMNS: string[] = [];
const NO_CUSTOM_COLUMNS: CustomColumnDefinition[] = [];
const FLAGS_COLUMN_WIDTH = 130;
const PROPERTY_COLUMN_WIDTH = 150;
const CUSTOM_COLUMN_WIDTH = 200;
const LAST_MODIFIED_COLUMN_WIDTH = 250;
const METADATA_COLUMN_NAME = "__metadata";
const ID_COLUMN_NAME = "@id";
const CHANGE_VECTOR_COLUMN_ID = "Change Vector";
const LAST_MODIFIED_COLUMN_ID = "Last Modified";
const COLLECTION_COLUMN_ID = "Collection";

const selectionColumn = createLazySelectionColumn<document>("Select all documents", "Select document");

const flagsColumn: ColumnDef<document> = { ...columnDocumentFlags, size: FLAGS_COLUMN_WIDTH };

type AppUrl = ReturnType<typeof useAppUrls>["appUrl"];

export function useDocumentsColumns({
    databaseName,
    collectionName,
    tableBodyWidthInPx,
    getPropertyPreviewResolver,
}: UseDocumentsColumnsProps) {
    const { appUrl } = useAppUrls();
    const isAllDocuments = collectionName === null;
    const propertyColumnsWidthInPx = tableBodyWidthInPx - columnCheckbox.size - FLAGS_COLUMN_WIDTH;

    const [appliedLayout, setAppliedLayout] = useState<AppliedColumnLayout | null>(() =>
        isAllDocuments ? null : documentsColumnLayoutStorage.load(databaseName, collectionName)
    );
    const [availableColumns, setAvailableColumns] = useState<string[]>([]);
    const [previewedColumns, setPreviewedColumns] = useState<string[]>([]);

    const customColumns = appliedLayout?.customColumns ?? NO_CUSTOM_COLUMNS;

    const fullBindings = useMemo(() => (appliedLayout ? getFullBindings(appliedLayout) : NO_COLUMNS), [appliedLayout]);

    const previewBindings = useMemo(
        () =>
            appliedLayout
                ? getPreviewBindings(appliedLayout, isAllDocuments).filter((x) => !fullBindings.includes(x))
                : NO_COLUMNS,
        [appliedLayout, isAllDocuments, fullBindings]
    );

    const propertyColumnNames = useMemo(
        () => (isAllDocuments ? availableColumns.filter((x) => x !== METADATA_COLUMN_NAME) : availableColumns),
        [isAllDocuments, availableColumns]
    );

    const propertyColumns = useDocumentColumnsProvider({
        columnNames: propertyColumnNames,
        availableWidth: propertyColumnsWidthInPx,
        columnsWithValues: isAllDocuments ? NO_COLUMNS : previewedColumns,
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
            columnOrder: appliedLayout?.columnOrder ?? NO_COLUMNS,
            columnPinning: { left: [columnCheckbox.id, ...(appliedLayout?.pinnedColumnIds ?? NO_COLUMNS)] },
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
                .filter((id) => id === ID_COLUMN_NAME || availableColumns.includes(id)),
    };
}

function getPreviewBindings(layout: AppliedColumnLayout, isAllDocuments: boolean): string[] {
    const nonPropertyColumnIds = new Set<string>([
        columnCheckbox.id,
        ID_COLUMN_NAME,
        columnDocumentFlags.id,
        ...(isAllDocuments ? [CHANGE_VECTOR_COLUMN_ID, LAST_MODIFIED_COLUMN_ID, COLLECTION_COLUMN_ID] : []),
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
    const getSize = (column: ColumnDef<document>) => column.size ?? PROPERTY_COLUMN_WIDTH;
    const isFixed = (column: ColumnDef<document>) =>
        column.id === columnCheckbox.id ||
        column.id === columnDocumentFlags.id ||
        column.id === LAST_MODIFIED_COLUMN_ID;

    const visibleColumns = columnDefs.filter((column) => columnVisibility[column.id] !== false);
    const stretchedColumns = visibleColumns.filter((column) => !isFixed(column));

    if (stretchedColumns.length === 0) {
        return columnDefs;
    }

    const fixedWidth = sumBy(visibleColumns.filter(isFixed), getSize);
    const scale = (availableWidth - fixedWidth) / sumBy(stretchedColumns, getSize);

    return columnDefs.map((column) =>
        stretchedColumns.includes(column)
            ? { ...column, size: Math.max(PROPERTY_COLUMN_WIDTH, Math.floor(getSize(column) * scale)) }
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
        size: CUSTOM_COLUMN_WIDTH,
        meta: { customColumn: column },
    };
}

function createMetadataColumns(databaseName: string, widthInPx: number, appUrl: AppUrl): ColumnDef<document>[] {
    const getSize = virtualTableUtils.getCellSizeProvider(widthInPx);

    return [
        {
            id: ID_COLUMN_NAME,
            header: "Id",
            accessorFn: (doc) => doc.getId(),
            cell: ({ getValue, row }) => (
                <CellDocumentId
                    id={getValue<string>()}
                    collection={row.original.getCollection()}
                    databaseName={databaseName}
                    hasHyperlink
                />
            ),
            size: getSize(30),
            enableHiding: false,
        },
        {
            id: CHANGE_VECTOR_COLUMN_ID,
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
            id: LAST_MODIFIED_COLUMN_ID,
            header: "Last Modified",
            accessorFn: (doc) => doc.__metadata.lastModified(),
            cell: DateFormatterCell,
            size: LAST_MODIFIED_COLUMN_WIDTH,
        },
        {
            id: COLLECTION_COLUMN_ID,
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
