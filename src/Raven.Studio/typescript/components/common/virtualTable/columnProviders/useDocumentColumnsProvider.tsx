import { ColumnDef } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { columnCheckbox, columnPreview } from "components/common/virtualTable/utils/commonColumnDefs";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";
import { useMemo } from "react";

// TODO Add Time Series column

interface UseDocumentColumnsProviderProps {
    // column names are derived from the documents unless columnNames is provided
    documents?: document[];
    columnNames?: string[];
    // when provided, the columns that do not fit start hidden and the visible ones are stretched to fill the width
    availableWidth?: number;
    databaseName?: string;
    hasPreview?: boolean;
    hasFlags?: boolean;
    hasCheckbox?: boolean;
    hasHyperlinkForIds?: boolean;
    // returns a fetch of the full value for the cell preview, undefined when the row already holds the whole value
    getPreviewValueResolver?: (doc: document, columnName: string) => (() => Promise<unknown>) | undefined;
}

interface CreateColumnsOptions extends UseDocumentColumnsProviderProps {
    databaseName: string;
    hasPreview: boolean;
    hasFlags: boolean;
    hasCheckbox: boolean;
    hasHyperlinkForIds: boolean;
}

export function useDocumentColumnsProvider(props: UseDocumentColumnsProviderProps) {
    const {
        documents,
        columnNames,
        availableWidth,
        hasHyperlinkForIds = true,
        hasPreview = false,
        hasFlags = false,
        hasCheckbox = false,
        getPreviewValueResolver,
    } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const databaseName = props.databaseName ?? activeDatabaseName;

    // column defs must keep their identity between renders, otherwise flexRender remounts every cell
    return useMemo(
        () =>
            createColumns({
                documents,
                columnNames,
                availableWidth,
                databaseName,
                hasHyperlinkForIds,
                hasPreview,
                hasFlags,
                hasCheckbox,
                getPreviewValueResolver,
            }),
        [
            documents,
            columnNames,
            availableWidth,
            databaseName,
            hasHyperlinkForIds,
            hasPreview,
            hasFlags,
            hasCheckbox,
            getPreviewValueResolver,
        ]
    );
}

function createColumns(options: CreateColumnsOptions) {
    const {
        documents,
        columnNames,
        availableWidth,
        databaseName,
        hasHyperlinkForIds,
        hasPreview,
        hasFlags,
        hasCheckbox,
        getPreviewValueResolver,
    } = options;

    const initialColumnVisibility: Record<string, boolean> = {};

    if (!documents && !columnNames) {
        return { columnDefs: [] as ColumnDef<document>[], initialColumnVisibility };
    }

    const leadingColumnDefs: ColumnDef<document>[] = [];

    if (hasCheckbox) {
        leadingColumnDefs.push(columnCheckbox as ColumnDef<document>);
        initialColumnVisibility[columnCheckbox.id] = true;
    }

    if (hasPreview) {
        leadingColumnDefs.push(columnPreview as ColumnDef<document>);
        initialColumnVisibility[columnPreview.header.toString()] = true;
    }

    const trailingColumnDefs: ColumnDef<document>[] = [];

    if (hasFlags) {
        trailingColumnDefs.push(columnDocumentFlags);
        initialColumnVisibility[columnDocumentFlags.id] = true;
    }

    const fixedColumnsWidth = [...leadingColumnDefs, ...trailingColumnDefs].reduce(
        (sum, column) => sum + (column.size ?? defaultSize),
        0
    );

    const allColumnNames = prioritizeColumnNames(columnNames ?? extractUniquePropertyNames(documents));
    const propertyColumnsWidth = availableWidth === undefined ? undefined : availableWidth - fixedColumnsWidth;

    const visibleColumnNames = getVisibleColumnNames(allColumnNames, propertyColumnsWidth);
    const propertyColumnSize = getPropertyColumnSize(propertyColumnsWidth, visibleColumnNames.length);

    const propertyColumnDefs = allColumnNames.map((columnName): ColumnDef<document> => {
        if (columnName === "__metadata") {
            initialColumnVisibility["@id"] = true;

            return {
                id: "@id",
                header: "@id",
                accessorFn: (x) => x?.getId(),
                cell: ({ getValue }) => (
                    <CellDocumentValue
                        value={getValue()}
                        databaseName={databaseName}
                        hasHyperlinkForIds={hasHyperlinkForIds}
                    />
                ),
                size: propertyColumnSize,
                enableHiding: false,
            };
        }

        initialColumnVisibility[columnName] = visibleColumnNames.includes(columnName);

        return {
            id: columnName,
            header: columnName,
            accessorFn: (doc) => doc.getValue(columnName),
            cell: ({ getValue, row }) => (
                <CellDocumentValue
                    value={getValue()}
                    databaseName={databaseName}
                    hasHyperlinkForIds={hasHyperlinkForIds}
                    resolvePreviewValue={getPreviewValueResolver?.(row.original, columnName)}
                />
            ),
            size: propertyColumnSize,
        };
    });

    return {
        columnDefs: [...leadingColumnDefs, ...propertyColumnDefs, ...trailingColumnDefs],
        initialColumnVisibility,
    };
}

// the columns are shown in order as long as they fit at the default size, @id is always shown
function getVisibleColumnNames(columnNames: string[], propertyColumnsWidth: number | undefined): string[] {
    if (propertyColumnsWidth === undefined) {
        return columnNames;
    }

    let remainingWidth = propertyColumnsWidth;

    return columnNames.filter((columnName) => {
        const isVisible = columnName === "__metadata" || remainingWidth >= 0;
        remainingWidth -= defaultSize;
        return isVisible;
    });
}

function getPropertyColumnSize(propertyColumnsWidth: number | undefined, visibleColumnsCount: number): number {
    if (propertyColumnsWidth === undefined || visibleColumnsCount === 0) {
        return defaultSize;
    }

    return Math.max(defaultSize, Math.floor(propertyColumnsWidth / visibleColumnsCount));
}

function prioritizeColumnNames(names: string[], prioritizedColumns = ["__metadata", "Name"]): string[] {
    const columnNames = [...names];

    // reverse the order of the prioritized columns
    // so they will be added to the beginning of the column list in the order they were provided
    prioritizedColumns.reverse().forEach((prioritizedColumn) => {
        if (columnNames.includes(prioritizedColumn)) {
            columnNames.splice(columnNames.indexOf(prioritizedColumn), 1);
            columnNames.unshift(prioritizedColumn);
        }
    });

    return columnNames;
}

function extractUniquePropertyNames(documents: document[]) {
    const uniquePropertyNames = new Set(documents.filter((x) => x).flatMap((x) => Object.keys(x)));

    if (!documents.every((x) => x && x.__metadata && x.getId())) {
        uniquePropertyNames.delete("__metadata");
    }

    return Array.from(uniquePropertyNames);
}

const defaultSize = 150;
