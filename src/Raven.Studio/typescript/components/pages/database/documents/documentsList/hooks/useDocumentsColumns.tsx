import {
    ColumnDef,
    ColumnOrderState,
    ColumnPinningState,
    functionalUpdate,
    OnChangeFn,
    Table as TanstackTable,
    VisibilityState,
} from "@tanstack/react-table";
import changeVectorUtils from "common/changeVectorUtils";
import { Checkbox } from "components/common/Checkbox";
import { useDocumentColumnsProvider } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import CellValue from "components/common/virtualTable/cells/CellValue";
import { CellWithCopy } from "components/common/virtualTable/cells/CellWithCopy";
import DateFormatterCell from "components/common/virtualTable/cells/CellDateFormatter";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import {
    AppliedColumnLayout,
    TableDisplaySettingsOptions,
} from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import {
    createCustomColumnAccessor,
    CustomColumnDefinition,
    getCustomColumnProperties,
} from "components/common/virtualTable/commonComponents/columnsSelect/customColumns";
import { columnCheckbox } from "components/common/virtualTable/utils/commonColumnDefs";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import { useAppUrls } from "components/hooks/useAppUrls";
import { DocumentsSelection } from "components/pages/database/documents/documentsList/hooks/useDocumentsSelection";
import { FullDocumentProvider } from "components/pages/database/documents/documentsList/hooks/useFullDocumentProvider";
import { documentsColumnLayoutStorage } from "components/pages/database/documents/documentsList/utils/documentsColumnLayoutStorage";
import { uniq } from "lodash";
import document from "models/database/documents/document";
import { ChangeEvent, RefObject, useMemo, useState } from "react";

interface UseDocumentsColumnsProps {
    databaseName: string;
    // null means all documents
    collectionName: string | null;
    tableBodyWidthInPx: number;
    selectionRef: RefObject<DocumentsSelection>;
    // provides the full values for the cell previews, the rows hold trimmed and stubbed ones
    fullDocumentProvider: FullDocumentProvider;
}

export interface DocumentsColumns {
    columnDefs: ColumnDef<document>[];
    tableState: {
        columnVisibility: VisibilityState;
        columnOrder: ColumnOrderState;
        columnPinning: ColumnPinningState;
    };
    onColumnVisibilityChange: OnChangeFn<VisibilityState>;
    onColumnOrderChange: OnChangeFn<ColumnOrderState>;
    onColumnPinningChange: OnChangeFn<ColumnPinningState>;
    // properties the documents preview has to include, the table reloads whenever they change
    previewBindings: string[];
    // properties the custom columns read, they are fetched in full because the preview may trim them
    fullBindings: string[];
    onAvailableColumns: (columnNames: string[]) => void;
    isCustomLayout: boolean;
    settingsOptions: TableDisplaySettingsOptions;
    getExportFields: (table: TanstackTable<document>) => string[];
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

type AppUrl = ReturnType<typeof useAppUrls>["appUrl"];

export function useDocumentsColumns({
    databaseName,
    collectionName,
    tableBodyWidthInPx,
    selectionRef,
    fullDocumentProvider,
}: UseDocumentsColumnsProps): DocumentsColumns {
    const { appUrl } = useAppUrls();
    const { getPropertyPreviewResolver, getCustomColumnPreviewResolver } = fullDocumentProvider;
    const isAllDocuments = collectionName === null;

    const [appliedLayout, setAppliedLayout] = useState<AppliedColumnLayout | null>(() =>
        documentsColumnLayoutStorage.load(databaseName, collectionName)
    );
    const [availableColumns, setAvailableColumns] = useState<string[]>(null);
    const [customColumns, setCustomColumns] = useState<CustomColumnDefinition[]>(
        () => appliedLayout?.customColumns ?? noCustomColumns
    );
    // only the columns toggled by the user, the rest follows the defaults (which may arrive later than the first render)
    const [columnVisibilityChanges, setColumnVisibilityChanges] = useState<VisibilityState>({});
    const [columnOrder, setColumnOrder] = useState<ColumnOrderState>(() => appliedLayout?.columnOrder ?? []);
    const [columnPinning, setColumnPinning] = useState<ColumnPinningState>(() =>
        appliedLayout ? { left: appliedLayout.pinnedColumnIds } : {}
    );

    // without an applied layout the server picks the previewed properties itself
    const previewBindings = useMemo(
        () => (appliedLayout ? getPreviewBindings(appliedLayout, isAllDocuments) : noColumns),
        [appliedLayout, isAllDocuments]
    );

    const fullBindings = useMemo(
        () => uniq(customColumns.flatMap((x) => getCustomColumnProperties(x.expression))),
        [customColumns]
    );

    const selectionColumn = useMemo(() => createSelectionColumn(selectionRef), [selectionRef]);

    const collectionColumns = useDocumentColumnsProvider({
        columnNames: isAllDocuments ? noColumns : (availableColumns ?? noColumns),
        availableWidth: tableBodyWidthInPx,
        databaseName,
        hasCheckbox: true,
        hasFlags: true,
        getPreviewValueResolver: getPropertyPreviewResolver,
    });

    const customColumnDefs = useMemo(
        () => customColumns.map((column) => createCustomColumn(column, databaseName, getCustomColumnPreviewResolver)),
        [customColumns, databaseName, getCustomColumnPreviewResolver]
    );

    const { columnDefs, defaultColumnVisibility } = useMemo(() => {
        if (!isAllDocuments) {
            return {
                columnDefs: withColumnsBeforeFlags(
                    collectionColumns.columnDefs.map((column) =>
                        column.id === columnCheckbox.id ? selectionColumn : column
                    ),
                    customColumnDefs
                ),
                defaultColumnVisibility: collectionColumns.initialColumnVisibility,
            };
        }

        const propertyColumnNames = (availableColumns ?? noColumns).filter((x) => x !== metadataColumnName);

        return {
            columnDefs: withColumnsBeforeFlags(
                createAllDocumentsColumns(databaseName, tableBodyWidthInPx, appUrl, selectionColumn),
                [
                    ...propertyColumnNames.map((x) =>
                        createPropertyColumn(x, databaseName, getPropertyPreviewResolver)
                    ),
                    ...customColumnDefs,
                ]
            ),
            // the metadata columns describe every document, the properties are available on demand
            defaultColumnVisibility: Object.fromEntries(propertyColumnNames.map((x) => [x, false])),
        };
    }, [
        isAllDocuments,
        databaseName,
        availableColumns,
        tableBodyWidthInPx,
        appUrl,
        collectionColumns,
        selectionColumn,
        customColumnDefs,
        getPropertyPreviewResolver,
    ]);

    // columns unknown to the saved layout (e.g. added to the documents later) stay hidden
    const savedVisibility = useMemo(
        () =>
            appliedLayout
                ? Object.fromEntries(
                      columnDefs
                          .filter((column) => column.enableHiding !== false)
                          .map((column) => [column.id, appliedLayout.visibleColumnIds.includes(column.id)])
                  )
                : null,
        [appliedLayout, columnDefs]
    );

    const columnVisibility = useMemo(
        () => ({ ...(savedVisibility ?? defaultColumnVisibility), ...columnVisibilityChanges }),
        [savedVisibility, defaultColumnVisibility, columnVisibilityChanges]
    );

    return {
        columnDefs,
        tableState: { columnVisibility, columnOrder, columnPinning },
        onColumnVisibilityChange: (updater) =>
            setColumnVisibilityChanges((prev) =>
                functionalUpdate(updater, { ...(savedVisibility ?? defaultColumnVisibility), ...prev })
            ),
        onColumnOrderChange: setColumnOrder,
        onColumnPinningChange: setColumnPinning,
        previewBindings,
        fullBindings,
        onAvailableColumns: (columnNames) => setAvailableColumns((prev) => mergeColumnNames(prev, columnNames)),
        isCustomLayout: appliedLayout !== null,
        settingsOptions: {
            customColumns: { columns: customColumns, onChange: setCustomColumns },
            onApplied: (layout) => {
                documentsColumnLayoutStorage.save(databaseName, collectionName, layout);
                setAppliedLayout(layout);
            },
            onRestoreDefaults: () => {
                documentsColumnLayoutStorage.clear(databaseName, collectionName);
                setAppliedLayout(null);
                setCustomColumns(noCustomColumns);
                setColumnVisibilityChanges({});
                setColumnOrder([]);
                setColumnPinning({});
            },
        },
        getExportFields: (table) => {
            const propertyNames = (availableColumns ?? noColumns).filter((x) => x !== metadataColumnName);

            return table
                .getVisibleLeafColumns()
                .map((column) => column.id)
                .filter((id) => id === idColumnName || propertyNames.includes(id));
        },
    };
}

// the visible property columns whose values the documents preview has to include,
// the metadata and custom columns are computed on the client (custom columns via the full bindings)
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

// every fetch reports only the columns of the documents it returned, so the columns seen so far are kept
// and the newly discovered ones are appended, otherwise scrolling would drop the columns already in the table
function mergeColumnNames(previous: string[] | null, next: string[]): string[] {
    if (previous === null) {
        return next;
    }

    const addedColumnNames = next.filter((name) => !previous.includes(name));

    return addedColumnNames.length === 0 ? previous : [...previous, ...addedColumnNames];
}

function withColumnsBeforeFlags(columnDefs: ColumnDef<document>[], columnsToInsert: ColumnDef<document>[]) {
    if (columnsToInsert.length === 0) {
        return columnDefs;
    }

    const flagsIndex = columnDefs.findIndex((x) => x.id === columnDocumentFlags.id);
    const insertIndex = flagsIndex === -1 ? columnDefs.length : flagsIndex;

    return [...columnDefs.slice(0, insertIndex), ...columnsToInsert, ...columnDefs.slice(insertIndex)];
}

function createSelectionColumn(selectionRef: RefObject<DocumentsSelection>): ColumnDef<document> {
    return {
        id: columnCheckbox.id,
        accessorFn: (x) => x,
        size: columnCheckbox.size,
        minSize: columnCheckbox.minSize,
        enableSorting: false,
        enableHiding: false,
        enableColumnFilter: false,
        enablePinning: false,
        header: () => {
            const { selectionState, toggleAll } = selectionRef.current;

            return (
                <Checkbox
                    selected={selectionState === "AllSelected"}
                    indeterminate={selectionState === "SomeSelected"}
                    toggleSelection={toggleAll}
                    aria-label="Select all documents"
                />
            );
        },
        cell: ({ row }) => (
            <Checkbox
                selected={row.getIsSelected()}
                toggleSelection={(e) => selectionRef.current.toggleRow(row.index, isShiftKeyPressed(e))}
                aria-label="Select document"
            />
        ),
    };
}

// React fires the change event of a checkbox from the click, so the mouse modifiers are available
function isShiftKeyPressed(e: ChangeEvent<HTMLInputElement>) {
    return e.nativeEvent instanceof MouseEvent && e.nativeEvent.shiftKey;
}

function createPropertyColumn(
    columnName: string,
    databaseName: string,
    getPreviewValueResolver: FullDocumentProvider["getPropertyPreviewResolver"]
): ColumnDef<document> {
    return {
        id: columnName,
        header: columnName,
        accessorFn: (doc) => doc.getValue(columnName),
        cell: ({ getValue, row }) => (
            <CellDocumentValue
                value={getValue()}
                databaseName={databaseName}
                hasHyperlinkForIds
                resolvePreviewValue={getPreviewValueResolver(row.original, columnName)}
            />
        ),
        size: propertyColumnWidth,
    };
}

function createCustomColumn(
    column: CustomColumnDefinition,
    databaseName: string,
    getPreviewValueResolver: FullDocumentProvider["getCustomColumnPreviewResolver"]
): ColumnDef<document> {
    const getValue = createCustomColumnAccessor(column.expression);
    const properties = getCustomColumnProperties(column.expression);

    return {
        id: column.id,
        header: column.header,
        accessorFn: (doc) => getValue(doc),
        cell: ({ getValue, row }) => (
            <CellDocumentValue
                value={getValue()}
                databaseName={databaseName}
                hasHyperlinkForIds
                resolvePreviewValue={getPreviewValueResolver(row.original, properties, getValue)}
            />
        ),
        size: customColumnWidth,
        meta: { customColumn: column },
    };
}

function createAllDocumentsColumns(
    databaseName: string,
    tableBodyWidthInPx: number,
    appUrl: AppUrl,
    selectionColumn: ColumnDef<document>
): ColumnDef<document>[] {
    const getSize = virtualTableUtils.getCellSizeProvider(tableBodyWidthInPx - columnCheckbox.size - flagsColumnWidth);

    return [
        selectionColumn,
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
        { ...columnDocumentFlags, size: flagsColumnWidth },
    ];
}
