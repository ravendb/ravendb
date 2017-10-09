/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class savedPatchesStorage {

    static getSavedPatchesWithIndexNameCheck(db: database): JQueryPromise<patchDto[]> {
        const savedPatches = this.getSavedPatches(db);
        return $.when(savedPatches);
    }

    static getSavedPatches(db: database): patchDto[] {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        let savedPatchesFromLocalStorage: patchDto[] = this.getSavedPatchesFromLocalStorage(localStorageName);

        if (savedPatchesFromLocalStorage == null || savedPatchesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            savedPatchesFromLocalStorage = [];
        }

        return savedPatchesFromLocalStorage;
    }

    static storeSavedPatches(db: database, savedPatches: patchDto[]): JQueryPromise<void>{
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        return $.when(localStorage.setObject(localStorageName, savedPatches));
    }

    static removeSavedPatchByName(db: database, name: string) {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        const savedPatchesFromLocalStorage: patchDto[] = this.getSavedPatchesFromLocalStorage(localStorageName);
        if (savedPatchesFromLocalStorage == null)
            return;

        const newSavedPatches = savedPatchesFromLocalStorage.filter((dto: patchDto) => dto.Name !== name);
        localStorage.setObject(localStorageName, newSavedPatches);
    }

    private static getLocalStorageKey(dbName: string) {
        return storageKeyProvider.storageKeyFor(`savedPatches.${dbName}`);
    }

    private static getSavedPatchesFromLocalStorage(localStorageName: string): patchDto[] {
        let savedPatchesFromLocalStorage: patchDto[] = null;
        try {
            savedPatchesFromLocalStorage = localStorage.getObject(localStorageName);
        } catch (err) {
            //no need to do anything
        }
        return savedPatchesFromLocalStorage;
    }

    static onDatabaseDeleted(qualifer: string, name: string) {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(name);
        localStorage.removeItem(localStorageName);
    }
}

export = savedPatchesStorage;
