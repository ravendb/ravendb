/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import indexMergeSuggestion = require("models/database/index/indexMergeSuggestion");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class mergedIndexesStorage {

    static getMergedIndex(db: database, mergedIndexName: string): indexMergeSuggestion {
        let newSuggestion: indexMergeSuggestion = null;

        try {
            if (!!mergedIndexName && mergedIndexName.indexOf(this.getStoragePrefixForDatabase(db.name)) == 0) {
                const suggestion: any = localStorage.getObject(mergedIndexName);//TODO: don't use any here
                localStorage.removeItem(mergedIndexName);
                newSuggestion = new indexMergeSuggestion(suggestion);
            }
        }
        catch (e) {
            return null;
        }

        return newSuggestion;
    }

    static saveMergedIndex(db: database, id: string, suggestion: indexMergeSuggestion) {
        const localStorageName = mergedIndexesStorage.getLocalStorageKey(db, id);
        localStorage.setObject(localStorageName, suggestion.toDto());
    }

    private static getStoragePrefixForDatabase(dbName: string) {
        return storageKeyProvider.storageKeyFor("mergedIndex." + dbName);
    }

    static getLocalStorageKey(db: database, id: string) {
        return mergedIndexesStorage.getStoragePrefixForDatabase(db.name) + "." + id;
    }

    static onDatabaseDeleted(qualifer: string, name: string) {
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
