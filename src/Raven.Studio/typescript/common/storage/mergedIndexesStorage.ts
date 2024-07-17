/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");


interface storedMergedIndex {
    definition: Raven.Client.Documents.Indexes.IndexDefinition;
    indexesToDelete: string[];
}

class mergedIndexesStorage {

    static getMergedIndex(db: database, mergedIndexName: string): storedMergedIndex | null {
        const localStorageKey = mergedIndexesStorage.getLocalStorageKey(db, mergedIndexName);
        const suggestion: storedMergedIndex = localStorage.getObject(localStorageKey);

        if (!suggestion) {
            return null;
        }

        if ("definition" in suggestion && "indexesToDelete" in suggestion) {
            return suggestion;
        } else {
            throw new Error("Saved index definition has malformed format");
        }
    }
    
    static deleteMergedIndex(db: database, mergedIndexName: string): void {
        const localStorageKey = mergedIndexesStorage.getLocalStorageKey(db, mergedIndexName);
        localStorage.removeItem(localStorageKey);
    }

    static saveMergedIndex(db: database | string, definition: Raven.Client.Documents.Indexes.IndexDefinition, indexesToDelete: string[]): string {
        const indexName = "merge-suggestion-" + new Date().getTime();
        const localStorageKey = mergedIndexesStorage.getLocalStorageKey(db, indexName);
        
        const toStore: storedMergedIndex = {
            indexesToDelete,
            definition
        };
        
        localStorage.setObject(localStorageKey, toStore);
        
        return indexName;
    }

    private static getStoragePrefixForDatabase(dbName: string) {
        return storageKeyProvider.storageKeyFor("mergedIndex." + dbName);
    }

    static getLocalStorageKey(db: database | string, id: string) {
        return mergedIndexesStorage.getStoragePrefixForDatabase(typeof db === "string" ? db : db.name) + "." + id;
    }

    static onDatabaseDeleted(qualifier: string, name: string) {
        const prefix = mergedIndexesStorage.getStoragePrefixForDatabase(name);

        const keysToDelete = [] as string[];

        for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(0);
            if (key.startsWith(prefix)) {
                keysToDelete.push(key);
            }
        }

        keysToDelete.forEach(key => {
            localStorage.removeItem(key);
        });
    }

}

export = mergedIndexesStorage;
