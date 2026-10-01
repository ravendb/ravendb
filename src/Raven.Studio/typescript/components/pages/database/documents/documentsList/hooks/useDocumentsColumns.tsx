import { ColumnDef, Table as TanstackTable, VisibilityState } from "@tanstack/react-table";
import changeVectorUtils from "common/changeVectorUtils";
import {
    getDocumentPropertyName,
    useDocumentColumnsProvider,
} from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
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
const METADATA_COLUMN_WIDTH_PERCENTAGES: Record<string, number> = {
    [ID_COLUMN_NAME]: 30,
    [CHANGE_VECTOR_COLUMN_ID]: 20,
    [COLLECTION_COLUMN_ID]: 25,
};

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
        () => (appliedLayout ? getPreviewBindings(appliedLayout).filter((x) => !fullBindings.includes(x)) : NO_COLUMNS),
        [appliedLayout, fullBindings]
    );

    const bindingsKey = JSON.stringify([[...previewBindings].sort(), [...fullBindings].sort()]);

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

    const defaultColumnVisibility = useSettledColumnVisibility(propertyColumns.initialColumnVisibility);

    const customColumnDefs = useMemo(
        () => customColumns.map((column) => createCustomColumn(column, databaseName)),
        [customColumns, databaseName]
    );

    const metadataColumnDefs = useMemo(
        () => (isAllDocuments ? createMetadataColumns(databaseName, appUrl) : []),
        [isAllDocuments, databaseName, appUrl]
    );

    const columnDefs = useMemo(
        () => [selectionColumn, ...metadataColumnDefs, ...propertyColumns.columnDefs, ...customColumnDefs, flagsColumn],
        [metadataColumnDefs, propertyColumns.columnDefs, customColumnDefs]
    );

    const tableState = useMemo(
        () => ({
            columnVisibility: appliedLayout ? getLayoutVisibility(columnDefs, appliedLayout) : defaultColumnVisibility,
            columnOrder: appliedLayout?.columnOrder ?? NO_COLUMNS,
            columnPinning: { left: [columnCheckbox.id, ...(appliedLayout?.pinnedColumnIds ?? NO_COLUMNS)] },
        }),
        [appliedLayout, columnDefs, defaultColumnVisibility]
    );

    const fittedColumnDefs = useMemo(
        () =>
            fitVisibleColumnsToWidth(
                columnDefs,
                tableState.columnVisibility,
                tableBodyWidthInPx,
                propertyColumnsWidthInPx
            ),
        [columnDefs, tableState.columnVisibility, tableBodyWidthInPx, propertyColumnsWidthInPx]
    );

    const defaultVisibleColumnIds = columnDefs
        .map((column) => column.id)
        .filter((id) => defaultColumnVisibility[id] !== false);

    return {
        columnDefs: fittedColumnDefs,
        tableState,
        previewBindings,
        fullBindings,
        bindingsKey,
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
                .map((column) => (column.id === ID_COLUMN_NAME ? ID_COLUMN_NAME : getDocumentPropertyName(column.id)))
                .filter((field) => field === ID_COLUMN_NAME || availableColumns.includes(field)),
    };
}

function useSettledColumnVisibility(columnVisibility: VisibilityState): VisibilityState {
    const [settledVisibility, setSettledVisibility] = useState(columnVisibility);
    const hasNewColumns = Object.keys(columnVisibility).some((id) => !(id in settledVisibility));

    if (!hasNewColumns) {
        return settledVisibility;
    }

    const nextSettledVisibility = { ...columnVisibility, ...settledVisibility };
    setSettledVisibility(nextSettledVisibility);

    return nextSettledVisibility;
}

function getPreviewBindings(layout: AppliedColumnLayout): string[] {
    return layout.visibleColumnIds.map(getDocumentPropertyName).filter((name) => name !== null);
}

function getFullBindings(layout: AppliedColumnLayout): string[] {
    const visibleCustomColumns = layout.customColumns.filter((column) => layout.visibleColumnIds.includes(column.id));

    return uniq(visibleCustomColumns.flatMap((column) => getCustomColumnProperties(column.expression)));
}

function fitVisibleColumnsToWidth(
    columnDefs: ColumnDef<document>[],
    columnVisibility: VisibilityState,
    availableWidth: number,
    propertyColumnsWidth: number
): ColumnDef<document>[] {
    const getMetadataColumnSize = virtualTableUtils.getCellSizeProvider(propertyColumnsWidth);
    const getSize = (column: ColumnDef<document>) => {
        const metadataWidthPercentage = METADATA_COLUMN_WIDTH_PERCENTAGES[column.id];
        if (metadataWidthPercentage && column.size == null) {
            return getMetadataColumnSize(metadataWidthPercentage);
        }
        return column.size ?? PROPERTY_COLUMN_WIDTH;
    };
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
    };
}

function createMetadataColumns(databaseName: string, appUrl: AppUrl): ColumnDef<document>[] {
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
        },
    ];
}
