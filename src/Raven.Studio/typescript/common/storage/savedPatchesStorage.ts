/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import genUtils = require("common/generalUtils");

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
        if (savedPatchesFromLocalStorage == null) {
            return;
        }

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

    static storePlaygroundScript(db: database | string, script: string) {
        const hash = genUtils.hashCode(script);
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db instanceof database ? db.name : db);
        let patches: storedPatchDto[] = savedPatchesStorage.getSavedPatchesFromLocalStorage(localStorageName) ?? [];
        const exists = patches.some((dto) => dto.Hash === hash);
        if (!exists) {
            const entry: storedPatchDto = {
                Name: `__playground_${hash}`,
                Query: script,
                RecentPatch: false,
                ModificationDate: new Date().toISOString(),
                Hash: hash,
            };
            patches = [entry, ...patches];
            localStorage.setObject(localStorageName, patches);
        }
        return hash;
    }

    static getPlaygroundScript(db: database | string, hash: number) {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(db instanceof database ? db.name : db);
        const patches: storedPatchDto[] = savedPatchesStorage.getSavedPatchesFromLocalStorage(localStorageName) ?? [];
        const entry = patches.find((dto) => dto.Hash === hash);
        return entry ? entry.Query : null;
    }

    static onDatabaseDeleted(qualifer: string, name: string) {
        const localStorageName = savedPatchesStorage.getLocalStorageKey(name);
        localStorage.removeItem(localStorageName);
    }
}

export = savedPatchesStorage;
