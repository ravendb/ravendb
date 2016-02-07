import database = require("models/resources/database");

class recentPatchesStorage {
    public static getRecentPatches(db: database): storedPatchDto[] {
        var localStorageName = db.recentPatchesLocalStorageName;
        var recentPatchesFromLocalStorage: storedPatchDto[] = this.getRecentPatchesFromLocalStorage(localStorageName);

        if (recentPatchesFromLocalStorage == null || recentPatchesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            recentPatchesFromLocalStorage = [];
        }

        return recentPatchesFromLocalStorage;
    }

    public static saveRecentPatches(db: database, recentPatches: storedPatchDto[]) {
        var localStorageName = db.recentPatchesLocalStorageName;
        localStorage.setObject(localStorageName, recentPatches);
    }

    public static removeIndexFromRecentPatches(db: database, indexName: string) {
        var localStorageName = db.recentPatchesLocalStorageName;
        var recentPatchesFromLocalStorage: storedPatchDto[] = this.getRecentPatchesFromLocalStorage(localStorageName);
        if (recentPatchesFromLocalStorage == null)
            return;
    
        var newRecentPatches = recentPatchesFromLocalStorage.filter((query: storedPatchDto) => query.PatchOnOption !== "Index" || query.SelectedItem !== indexName);
        localStorage.setObject(localStorageName, newRecentPatches);
    }

    public static removeRecentPatches(db: database) {
        var localStorageName = db.recentPatchesLocalStorageName;
        localStorage.setObject(localStorageName, []);
    }

    private static getRecentPatchesFromLocalStorage(localStorageName: string): storedPatchDto[]  {
        var recentPatchesFromLocalStorage: storedPatchDto[] = null;
        try {
            recentPatchesFromLocalStorage = localStorage.getObject(localStorageName);
        } catch(err) {
            //no need to do anything
        }
        return recentPatchesFromLocalStorage;
    }
}

export = recentPatchesStorage;
