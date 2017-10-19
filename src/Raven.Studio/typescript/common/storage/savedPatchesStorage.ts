/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class savedPatchesStorage {

    static getSavedPatches(db: database): storedPatchDto[] {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        let savedPatchesFromLocalStorage: storedPatchDto[] = this.getSavedPatchesFromLocalStorage(localStorageName);

        if (savedPatchesFromLocalStorage == null || savedPatchesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            savedPatchesFromLocalStorage = [];
        }

        return savedPatchesFromLocalStorage;
    }

    static storeSavedPatches(db: database, savedPatches: storedPatchDto[]){
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, savedPatches);
    }

    static removeSavedPatchByHash(db: database, hash: number) {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db.name);
        const savedPatchesFromLocalStorage: storedPatchDto[] = this.getSavedPatchesFromLocalStorage(localStorageName);
        if (savedPatchesFromLocalStorage == null)
            return;

        const newSavedPatches = savedPatchesFromLocalStorage.filter((dto: storedPatchDto) => dto.Hash !== hash);
        localStorage.setObject(localStorageName, newSavedPatches);
    }

    private static getLocalStorageKey(dbName: string) {
        return storageKeyProvider.storageKeyFor(`savedPatches.${dbName}`);
    }

    private static getSavedPatchesFromLocalStorage(localStorageName: string): storedPatchDto[] {
        let savedPatchesFromLocalStorage: storedPatchDto[] = null;
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
