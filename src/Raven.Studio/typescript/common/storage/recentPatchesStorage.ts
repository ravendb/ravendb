import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class recentPatchesStorage {
    static getRecentPatches(db: database): storedPatchDto[] {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(db.name);
        let recentPatchesFromLocalStorage: storedPatchDto[] = this.getRecentPatchesFromLocalStorage(localStorageName);

        if (recentPatchesFromLocalStorage == null || recentPatchesFromLocalStorage instanceof Array === false) {
            this.removeRecentPatches(db);
            recentPatchesFromLocalStorage = [];
        }

        return recentPatchesFromLocalStorage;
    }

    static saveRecentPatches(db: database, recentPatches: storedPatchDto[]) {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, recentPatches);
    }

    static removeIndexFromRecentPatches(db: database, indexName: string) {
        recentPatchesStorage.removeIndexFromRecentPatchesByName(db.name, indexName);
    }

    private static removeIndexFromRecentPatchesByName(dbName: string, indexName: string) {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(dbName);
        const recentPatchesFromLocalStorage: storedPatchDto[] = this.getRecentPatchesFromLocalStorage(localStorageName);
        if (recentPatchesFromLocalStorage == null)
            return;
    
        const newRecentPatches = recentPatchesFromLocalStorage.filter((query: storedPatchDto) => query.PatchOnOption !== "Index" || query.SelectedItem !== indexName);
        localStorage.setObject(localStorageName, newRecentPatches);
    }

    static removeRecentPatches(db: database) {
        localStorage.removeItem(recentPatchesStorage.getLocalStorageKey(db.name));
    }

    private static getLocalStorageKey(dbName: string) {
        return storageKeyProvider.storageKeyFor("recentPatches." + dbName);
    }

    private static getRecentPatchesFromLocalStorage(localStorageName: string): storedPatchDto[]  {
        let recentPatchesFromLocalStorage: storedPatchDto[] = null;
        try {
            recentPatchesFromLocalStorage = localStorage.getObject(localStorageName);
        } catch(err) {
            //no need to do anything
        }
        return recentPatchesFromLocalStorage;
    }

    static onResourceDeleted(qualifer: string, name: string) {
        if (qualifer === database.qualifier) {
            const localStorageName = recentPatchesStorage.getLocalStorageKey(name);
            localStorage.removeItem(localStorageName);
        }
    }

    static onIndexDeleted(dbName: string, indexName: string) {
        recentPatchesStorage.removeIndexFromRecentPatchesByName(dbName, indexName);
    }
}

export = recentPatchesStorage;
