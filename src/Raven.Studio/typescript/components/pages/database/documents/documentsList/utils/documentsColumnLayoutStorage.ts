import storageKeyProvider from "common/storage/storageKeyProvider";
import { AppliedColumnLayout } from "components/common/virtualTable/commonComponents/columnsSelect/TableDisplaySettings";

function getStorageKey(databaseName: string, collectionName: string) {
    return storageKeyProvider.storageKeyFor(`documents-columns-${databaseName}.[${collectionName}]`);
}

export const documentsColumnLayoutStorage = {
    load(databaseName: string, collectionName: string): AppliedColumnLayout | null {
        return JSON.parse(localStorage.getItem(getStorageKey(databaseName, collectionName)));
    },
    save(databaseName: string, collectionName: string, layout: AppliedColumnLayout) {
        localStorage.setItem(getStorageKey(databaseName, collectionName), JSON.stringify(layout));
    },
    clear(databaseName: string, collectionName: string) {
        localStorage.removeItem(getStorageKey(databaseName, collectionName));
    },
};
