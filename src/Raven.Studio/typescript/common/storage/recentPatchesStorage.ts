import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");
import getIndexNamesComamnd = require("commands/database/index/getIndexNamesCommand");

class recentPatchesStorage {
    static getRecentPatchesWithIndexNamesCheck(db: database): JQueryPromise<storedPatchDto[]> {
        const recentPatches = this.getRecentPatches(db);

        const task = $.Deferred<storedPatchDto[]>();

        new getIndexNamesComamnd(db)
            .execute()
            .done((indexNames: string[]) => {
                const filteredPatches = recentPatches.filter(x => x.PatchOnOption !== "Index" || _.includes(indexNames, x.SelectedItem));

                if (filteredPatches.length !== recentPatches.length) {
                    this.saveRecentPatches(db, filteredPatches);
                }

                task.resolve(filteredPatches);
            })
            .fail((response) => task.reject());

        return task;
    }

    static saveRecentPatches(db: database, recentPatches: storedPatchDto[]) {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, recentPatches);
    }

    private static getRecentPatches(db: database): storedPatchDto[] {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(db.name);
        let recentPatchesFromLocalStorage: storedPatchDto[] = this.getRecentPatchesFromLocalStorage(localStorageName);

        if (recentPatchesFromLocalStorage == null || recentPatchesFromLocalStorage instanceof Array === false) {
            this.removeRecentPatches(db);
            recentPatchesFromLocalStorage = [];
        }

        return recentPatchesFromLocalStorage;
    }

    private static removeIndexFromRecentPatches(db: database, indexName: string) {
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

    static onDatabaseDeleted(qualifer: string, name: string) {
        const localStorageName = recentPatchesStorage.getLocalStorageKey(name);
        localStorage.removeItem(localStorageName);
    }

}

export = recentPatchesStorage;
