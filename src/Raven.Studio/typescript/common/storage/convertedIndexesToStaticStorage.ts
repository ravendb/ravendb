/// <reference path="../../../typings/tsd.d.ts" />

import storageKeyProvider = require("common/storage/storageKeyProvider");

interface StoredConvertedToStaticIndex {
    definition: Raven.Client.Documents.Indexes.IndexDefinition;
}

class convertedIndexesToStaticStorage {
    static getIndex(databaseName: string, indexName: string): StoredConvertedToStaticIndex | null {
        const localStorageKey = convertedIndexesToStaticStorage.getLocalStorageKey(databaseName, indexName);
        const index: StoredConvertedToStaticIndex = localStorage.getObject(localStorageKey);

        if (!index) {
            return null;
        }

        if ("definition" in index) {
            return index;
        } else {
            throw new Error("Saved index definition has malformed format");
        }
    }

    static saveIndex(databaseName: string, definition: Raven.Client.Documents.Indexes.IndexDefinition): string {
        const indexName = "converted-to-static-" + new Date().getTime();
        const localStorageKey = convertedIndexesToStaticStorage.getLocalStorageKey(databaseName, indexName);

        const toStore: StoredConvertedToStaticIndex = {
            definition,
        };

        localStorage.setObject(localStorageKey, toStore);

        return indexName;
    }

    private static getStoragePrefixForDatabase(dbName: string) {
        return storageKeyProvider.storageKeyFor("convertedToStaticIndex." + dbName);
    }

    private static getLocalStorageKey(databaseName: string, id: string) {
        return convertedIndexesToStaticStorage.getStoragePrefixForDatabase(databaseName) + "." + id;
    }

    static onDatabaseDeleted(_: string, name: string) {
        const prefix = convertedIndexesToStaticStorage.getStoragePrefixForDatabase(name);

        const keysToDelete = [] as string[];

        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(0);
            if (key.startsWith(prefix)) {
                keysToDelete.push(key);
            }
        }

        keysToDelete.forEach((key) => {
            localStorage.removeItem(key);
        });
    }
}

export = convertedIndexesToStaticStorage;
