import { CellContext, ColumnDef } from "@tanstack/react-table";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import CellDocumentId from "components/common/virtualTable/cells/CellDocumentId";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { columnCheckbox, columnPreview } from "components/common/virtualTable/utils/commonColumnDefs";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";
import { ReactElement, useMemo } from "react";

// TODO Add Time Series column

type PreviewValueResolverProvider = (doc: document, propertyName: string) => (() => Promise<unknown>) | undefined;

interface UseDocumentColumnsProviderProps {
    documents?: document[];
    columnNames?: string[];
    availableWidth?: number;
    columnsWithValues?: string[];
    databaseName?: string;
    hasPreview?: boolean;
    hasFlags?: boolean;
    hasCheckbox?: boolean;
    hasHyperlinkForIds?: boolean;
    getPreviewValueResolver?: PreviewValueResolverProvider;
}

interface DocumentColumns {
    leadingColumnDefs: ColumnDef<document>[];
    trailingColumnDefs: ColumnDef<document>[];
    propertyColumnDefs: ColumnDef<document>[];
    propertyNames: string[];
}

type DocumentCell = (context: CellContext<document, unknown>) => ReactElement;

const METADATA_PROPERTY_NAME = "__metadata";
const ID_COLUMN_ID = "@id";
const PROPERTY_COLUMN_ID_PREFIX = "property:";
const defaultSize = 150;

export function getDocumentPropertyColumnId(propertyName: string): string {
    return propertyName === METADATA_PROPERTY_NAME ? ID_COLUMN_ID : PROPERTY_COLUMN_ID_PREFIX + propertyName;
}

export function getDocumentPropertyName(columnId: string): string | null {
    return columnId.startsWith(PROPERTY_COLUMN_ID_PREFIX) ? columnId.slice(PROPERTY_COLUMN_ID_PREFIX.length) : null;
}

export function useDocumentColumnsProvider(props: UseDocumentColumnsProviderProps) {
    const {
        documents,
        columnNames,
        availableWidth,
        columnsWithValues,
        hasHyperlinkForIds = true,
        hasPreview = false,
        hasFlags = false,
        hasCheckbox = false,
        getPreviewValueResolver,
    } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const databaseName = props.databaseName ?? activeDatabaseName;

    const idCell = useMemo(() => createIdCell(databaseName, hasHyperlinkForIds), [databaseName, hasHyperlinkForIds]);
    const propertyCell = useMemo(
        () => createPropertyCell(databaseName, hasHyperlinkForIds, getPreviewValueResolver),
        [databaseName, hasHyperlinkForIds, getPreviewValueResolver]
    );

    const columns = useMemo(
        () => createColumns({ documents, columnNames, hasPreview, hasFlags, hasCheckbox, idCell, propertyCell }),
        [documents, columnNames, hasPreview, hasFlags, hasCheckbox, idCell, propertyCell]
    );

    return useMemo(
        () => fitColumnsToWidth(columns, availableWidth, columnsWithValues),
        [columns, availableWidth, columnsWithValues]
    );
}

function createIdCell(databaseName: string, hasHyperlinkForIds: boolean): DocumentCell {
    return function IdCell({ getValue, row }) {
        return (
            <CellDocumentId
                id={getValue<string>()}
                collection={row.original?.getCollection()}
                databaseName={databaseName}
                hasHyperlink={hasHyperlinkForIds}
            />
        );
    };
}

function createPropertyCell(
    databaseName: string,
    hasHyperlinkForIds: boolean,
    getPreviewValueResolver: PreviewValueResolverProvider | undefined
): DocumentCell {
    return function PropertyCell({ getValue, row, column }) {
        return (
            <CellDocumentValue
                value={getValue()}
                databaseName={databaseName}
                hasHyperlinkForIds={hasHyperlinkForIds}
                resolvePreviewValue={getPreviewValueResolver?.(row.original, getDocumentPropertyName(column.id))}
            />
        );
    };
}

interface CreateColumnsOptions {
    documents: document[] | undefined;
    columnNames: string[] | undefined;
    hasPreview: boolean;
    hasFlags: boolean;
    hasCheckbox: boolean;
    idCell: DocumentCell;
    propertyCell: DocumentCell;
}

function createColumns(options: CreateColumnsOptions): DocumentColumns | null {
    const { documents, columnNames, hasPreview, hasFlags, hasCheckbox, idCell, propertyCell } = options;

    if (!documents && !columnNames) {
        return null;
    }

    const leadingColumnDefs: ColumnDef<document>[] = [];

    if (hasCheckbox) {
        leadingColumnDefs.push(columnCheckbox as ColumnDef<document>);
    }

    if (hasPreview) {
        leadingColumnDefs.push(columnPreview as ColumnDef<document>);
    }

    const trailingColumnDefs: ColumnDef<document>[] = hasFlags ? [columnDocumentFlags] : [];

    const propertyNames = prioritizeColumnNames(columnNames ?? extractUniquePropertyNames(documents));

    const propertyColumnDefs = propertyNames.map((propertyName): ColumnDef<document> => {
        if (propertyName === METADATA_PROPERTY_NAME) {
            return {
                id: ID_COLUMN_ID,
                header: ID_COLUMN_ID,
                accessorFn: (x) => x?.getId(),
                cell: idCell,
                enableHiding: false,
            };
        }

        return {
            id: getDocumentPropertyColumnId(propertyName),
            header: propertyName,
            accessorFn: (doc) => doc.getValue(propertyName),
            cell: propertyCell,
        };
    });

    return { leadingColumnDefs, trailingColumnDefs, propertyColumnDefs, propertyNames };
}

function fitColumnsToWidth(
    columns: DocumentColumns | null,
    availableWidth: number | undefined,
    columnsWithValues: string[] | undefined
) {
    const initialColumnVisibility: Record<string, boolean> = {};

    if (!columns) {
        return { columnDefs: [] as ColumnDef<document>[], initialColumnVisibility };
    }

    const { leadingColumnDefs, trailingColumnDefs, propertyColumnDefs, propertyNames } = columns;

    [...leadingColumnDefs, ...trailingColumnDefs].forEach((column) => {
        initialColumnVisibility[getColumnId(column)] = true;
    });

    const fixedColumnsWidth = [...leadingColumnDefs, ...trailingColumnDefs].reduce(
        (sum, column) => sum + (column.size ?? defaultSize),
        0
    );
    const propertyColumnsWidth = availableWidth === undefined ? undefined : availableWidth - fixedColumnsWidth;

    const visibleColumnCandidates = columnsWithValues
        ? propertyNames.filter((x) => x === METADATA_PROPERTY_NAME || columnsWithValues.includes(x))
        : propertyNames;
    const visiblePropertyNames = getVisibleColumnNames(visibleColumnCandidates, propertyColumnsWidth);
    const propertyColumnSize = getPropertyColumnSize(propertyColumnsWidth, visiblePropertyNames.length);

    propertyNames.forEach((propertyName) => {
        initialColumnVisibility[getDocumentPropertyColumnId(propertyName)] =
            propertyName === METADATA_PROPERTY_NAME || visiblePropertyNames.includes(propertyName);
    });

    return {
        columnDefs: [
            ...leadingColumnDefs,
            ...propertyColumnDefs.map((column) => ({ ...column, size: propertyColumnSize })),
            ...trailingColumnDefs,
        ],
        initialColumnVisibility,
    };
}

function getColumnId(column: ColumnDef<document>): string {
    return column.id ?? column.header.toString();
}

function getVisibleColumnNames(columnNames: string[], propertyColumnsWidth: number | undefined): string[] {
    if (propertyColumnsWidth === undefined) {
        return columnNames;
    }

    let remainingWidth = propertyColumnsWidth;

    return columnNames.filter((columnName) => {
        remainingWidth -= defaultSize;
        return columnName === METADATA_PROPERTY_NAME || remainingWidth >= 0;
    });
}

function getPropertyColumnSize(propertyColumnsWidth: number | undefined, visibleColumnsCount: number): number {
    if (propertyColumnsWidth === undefined || visibleColumnsCount === 0) {
        return defaultSize;
    }

    return Math.max(defaultSize, Math.floor(propertyColumnsWidth / visibleColumnsCount));
}

function prioritizeColumnNames(names: string[], prioritizedColumns = [METADATA_PROPERTY_NAME, "Name"]): string[] {
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
        uniquePropertyNames.delete(METADATA_PROPERTY_NAME);
    }

    return Array.from(uniquePropertyNames);
}
