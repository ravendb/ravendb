import { ColumnDef } from "@tanstack/react-table";
import { todo } from "common/developmentHelper";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import CellDocumentValue from "components/common/virtualTable/cells/CellDocumentValue";
import { columnCheckbox, columnPreview } from "components/common/virtualTable/utils/commonColumnDefs";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import { useAppSelector } from "components/store";
import document from "models/database/documents/document";

todo("Feature", "Damian", "Add Time Series column");

interface UseDocumentColumnsProviderProps {
    documents: document[];
    availableWidth?: number;
    databaseName?: string;
    hasPreview?: boolean;
    hasFlags?: boolean;
    hasCheckbox?: boolean;
    hasHyperlinkForIds?: boolean;
}

export function useDocumentColumnsProvider(props: UseDocumentColumnsProviderProps) {
    const { documents, hasHyperlinkForIds = true, hasPreview = false, hasFlags = false, hasCheckbox = false } = props;

    const activeDatabaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const databaseName = props.databaseName ?? activeDatabaseName;

    const initialColumnVisibility: Record<string, boolean> = {};
    let availableWidth = props.availableWidth ?? window.innerWidth;

    const getColumnDefs = (): ColumnDef<document>[] => {
        if (!documents) {
            return [];
        }

        let columnsDefs: ColumnDef<document>[] = [];

        if (hasCheckbox) {
            columnsDefs.push(columnCheckbox as ColumnDef<document>);
            initialColumnVisibility[columnCheckbox.header.toString()] = true;
            availableWidth -= columnCheckbox.size;
        }

        if (hasPreview) {
            columnsDefs.push(columnPreview as ColumnDef<document>);
            initialColumnVisibility[columnPreview.header.toString()] = true;
            availableWidth -= columnPreview.size;
        }

        if (hasFlags) {
            columnsDefs.push(columnDocumentFlags);
            initialColumnVisibility[columnDocumentFlags.header.toString()] = true;
            availableWidth -= columnDocumentFlags.size ?? defaultSize;
        }

        const allColumnNames = findColumnNames(documents);

        const defaultColumnDefs = allColumnNames.map((columnName): ColumnDef<document> => {
            if (columnName === "__metadata") {
                initialColumnVisibility["@id"] = true;
                availableWidth -= defaultSize;

                return {
                    header: "@id",
                    accessorFn: (x) => x,
                    cell: ({ getValue }) => (
                        <CellDocumentValue
                            value={getValue<document>().getId()}
                            databaseName={databaseName}
                            hasHyperlinkForIds={hasHyperlinkForIds}
                        />
                    ),
                    enableHiding: false,
                };
            }

            initialColumnVisibility[columnName] = availableWidth >= 0;
            availableWidth -= defaultSize;

            return {
                header: columnName,
                accessorFn: (doc) => doc.getValue(columnName),
                cell: ({ getValue }) => (
                    <CellDocumentValue
                        value={getValue()}
                        databaseName={databaseName}
                        hasHyperlinkForIds={hasHyperlinkForIds}
                    />
                ),
            };
        });

        columnsDefs = [...columnsDefs, ...defaultColumnDefs];

        // move flags column to the end
        if (hasFlags) {
            columnsDefs = columnsDefs.filter((x) => x.id !== columnDocumentFlags.id.toString());
            columnsDefs.push(columnDocumentFlags);
        }

        return columnsDefs;
    };

    const columnDefs = getColumnDefs();

    return {
        columnDefs,
        initialColumnVisibility,
    };
}

function findColumnNames(documents: document[], prioritizedColumns = ["__metadata", "Name"]): string[] {
    const columnNames = extractUniquePropertyNames(documents);

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
