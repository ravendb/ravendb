/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require("models/database");
import indexDefinition = require("models/indexDefinition");

class mergedIndexesStorage {
    public static getMergedIndex(db: database, mergedIndexFullName: string): indexDefinition {
        var newIndexDefinition = null;

        try {
            if (!!mergedIndexFullName && mergedIndexFullName.indexOf(db.mergedIndexLocalStoragePrefix) == 0) {
                var indexFromLocalStorage: indexDefinitionDto = localStorage.getObject(mergedIndexFullName);
                newIndexDefinition = new indexDefinition(indexFromLocalStorage);
            }
        } catch (e) {
            return null;
        }

        return newIndexDefinition;
    }

    public static saveMergedIndex(db: database, mergedIndexName: string, mergedIndex: indexDefinition): string {
        var localStorageName = db.mergedIndexLocalStoragePrefix + '.' + mergedIndexName;
        localStorage.setObject(localStorageName, mergedIndex.toDto());
        return localStorageName;
    }

    public static removeMergedIndex(mergedIndexFullName: string) {
        localStorage.removeItem(mergedIndexFullName);
    }
}

export = mergedIndexesStorage;