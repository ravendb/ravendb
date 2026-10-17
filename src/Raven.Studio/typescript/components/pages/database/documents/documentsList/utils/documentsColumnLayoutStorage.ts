import genUtils from "common/generalUtils";
import storageKeyProvider from "common/storage/storageKeyProvider";
import { getDocumentPropertyColumnId } from "components/common/virtualTable/columnProviders/useDocumentColumnsProvider";
import { AppliedColumnLayout } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";
import {
    createCustomColumnId,
    CustomColumnDefinition,
} from "components/common/virtualTable/commonComponents/columnsSelect/customColumns";
import { columnDocumentFlags } from "components/common/virtualTable/utils/documentColumnDefs";
import document from "models/database/documents/document";

function getStorageKey(databaseName: string, collectionName: string) {
    return storageKeyProvider.storageKeyFor(`custom-columns-${databaseName}.[${collectionName}]`);
}

function parseStoredValue(key: string): unknown {
    try {
        return JSON.parse(localStorage.getItem(key));
    } catch {
        return null;
    }
}

function isStringArray(value: unknown): value is string[] {
    return Array.isArray(value) && value.every((x) => typeof x === "string");
}

function isCustomColumnDefinition(value: unknown): value is CustomColumnDefinition {
    const column = value as CustomColumnDefinition;
    return (
        column != null &&
        typeof column.id === "string" &&
        typeof column.header === "string" &&
        typeof column.expression === "string"
    );
}

function isColumnLayout(value: unknown): value is AppliedColumnLayout {
    const layout = value as AppliedColumnLayout;
    return (
        layout != null &&
        isStringArray(layout.visibleColumnIds) &&
        isStringArray(layout.columnOrder) &&
        isStringArray(layout.pinnedColumnIds) &&
        Array.isArray(layout.customColumns) &&
        layout.customColumns.every(isCustomColumnDefinition)
    );
}

function isLegacyLayout(value: unknown): value is serializedColumnDto[] {
    return Array.isArray(value) && value.every((x: serializedColumnDto) => typeof x?.column?.type === "string");
}

function getLegacyColumnId(
    { type, header, serializedValue }: serializedColumnDto["column"],
    customColumns: CustomColumnDefinition[]
): string | null {
    if (type === "flags") {
        return columnDocumentFlags.id;
    }

    if (typeof serializedValue !== "string") {
        return null;
    }

    switch (type) {
        case "hyperlink":
            return serializedValue === document.customColumnName
                ? getDocumentPropertyColumnId("__metadata")
                : getDocumentPropertyColumnId(serializedValue);
        case "text":
            return getDocumentPropertyColumnId(serializedValue);
        case "custom": {
            const customColumn: CustomColumnDefinition = {
                id: createCustomColumnId(),
                header: genUtils.unescapeHtml(header ?? ""),
                expression: serializedValue,
            };
            customColumns.push(customColumn);
            return customColumn.id;
        }
        default:
            return null;
    }
}

function convertLegacyLayout(legacyColumns: serializedColumnDto[]): AppliedColumnLayout {
    const customColumns: CustomColumnDefinition[] = [];
    const columnOrder: string[] = [];
    const visibleColumnIds: string[] = [];

    legacyColumns.forEach(({ column, visible }) => {
        const id = getLegacyColumnId(column, customColumns);
        if (id === null || columnOrder.includes(id)) {
            return;
        }

        columnOrder.push(id);
        if (visible) {
            visibleColumnIds.push(id);
        }
    });

    return { visibleColumnIds, columnOrder, pinnedColumnIds: [], customColumns };
}

export const documentsColumnLayoutStorage = {
    load(databaseName: string, collectionName: string): AppliedColumnLayout | null {
        const key = getStorageKey(databaseName, collectionName);
        const value = parseStoredValue(key);

        if (value == null) {
            return null;
        }

        if (isColumnLayout(value)) {
            return value;
        }

        if (isLegacyLayout(value)) {
            const layout = convertLegacyLayout(value);
            documentsColumnLayoutStorage.save(databaseName, collectionName, layout);
            return layout;
        }

        localStorage.removeItem(key);
        return null;
    },
    save(databaseName: string, collectionName: string, layout: AppliedColumnLayout) {
        localStorage.setItem(getStorageKey(databaseName, collectionName), JSON.stringify(layout));
    },
    clear(databaseName: string, collectionName: string) {
        localStorage.removeItem(getStorageKey(databaseName, collectionName));
    },
};
