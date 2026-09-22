import storageKeyProvider from "common/storage/storageKeyProvider";
import { AppliedColumnLayout } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";

const storagePrefix = "documents-columns-";

// the layout is saved per collection, the all documents view always uses the defaults
function getStorageKey(databaseName: string, collectionName: string | null): string | null {
    if (collectionName === null) {
        return null;
    }

    return storageKeyProvider.storageKeyFor(`${storagePrefix}${databaseName}.[${collectionName}]`);
}

function isColumnLayout(value: unknown): value is AppliedColumnLayout {
    if (!value || typeof value !== "object") {
        return false;
    }

    const layout = value as Record<string, unknown>;
    return ["visibleColumnIds", "columnOrder", "pinnedColumnIds", "customColumns"].every((key) =>
        Array.isArray(layout[key])
    );
}

export const documentsColumnLayoutStorage = {
    load(databaseName: string, collectionName: string | null): AppliedColumnLayout | null {
        const key = getStorageKey(databaseName, collectionName);
        if (!key) {
            return null;
        }

        try {
            const layout: unknown = JSON.parse(localStorage.getItem(key));
            return isColumnLayout(layout) ? layout : null;
        } catch {
            return null;
        }
    },

    save(databaseName: string, collectionName: string | null, layout: AppliedColumnLayout) {
        const key = getStorageKey(databaseName, collectionName);
        if (key) {
            localStorage.setItem(key, JSON.stringify(layout));
        }
    },

    clear(databaseName: string, collectionName: string | null) {
        const key = getStorageKey(databaseName, collectionName);
        if (key) {
            localStorage.removeItem(key);
        }
    },
};
