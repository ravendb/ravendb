/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require("models/database");
import indexMergeSuggestion = require("models/indexMergeSuggestion");

class mergedIndexesStorage {
    public static getMergedIndex(db: database, mergedIndexFullName: string): indexMergeSuggestion {
        var newSuggestion: indexMergeSuggestion = null;

        try {
            if (!!mergedIndexFullName && mergedIndexFullName.indexOf(db.mergedIndexLocalStoragePrefix) == 0) {
                var suggestion: suggestionDto = localStorage.getObject(mergedIndexFullName);
                localStorage.removeItem(mergedIndexFullName);
                newSuggestion = new indexMergeSuggestion(suggestion);
            }
        }
        catch (e) {
            return null;
        }

        return newSuggestion;
    }

    public static saveMergedIndex(db: database, mergedIndexName: string, suggestion: indexMergeSuggestion): string {
        var localStorageName = mergedIndexesStorage.getLocalStorageName(db, mergedIndexName);
        localStorage.setObject(localStorageName, suggestion.toDto());
        return localStorageName;
    }

    public static getLocalStorageName(db: database, mergedIndexName: string) {
        return db.mergedIndexLocalStoragePrefix + '.' + mergedIndexName;
    }
}

export = mergedIndexesStorage;