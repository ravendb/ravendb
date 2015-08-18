/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require("models/database");

class recentQueriesStorage {
    public static getRecentQueries(db: database): storedQueryDto[] {
        var localStorageName = db.recentQueriesLocalStorageName;
	    var recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);

        if (recentQueriesFromLocalStorage == null || recentQueriesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            recentQueriesFromLocalStorage = [];
        }

        return recentQueriesFromLocalStorage;
    }

    public static saveRecentQueries(db: database, recentQueries: storedQueryDto[]) {
        var localStorageName = db.recentQueriesLocalStorageName;
        localStorage.setObject(localStorageName, recentQueries);
    }

    public static removeIndexFromRecentQueries(db: database, indexName: string) {
        var localStorageName = db.recentQueriesLocalStorageName;
		var recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);
	    if (recentQueriesFromLocalStorage == null)
		    return;

        var newRecentQueries = recentQueriesFromLocalStorage.filter((query: storedQueryDto) => query.IndexName != indexName);
        localStorage.setObject(localStorageName, newRecentQueries);
    }

	public static removeRecentQueries(db: database) {
		var localStorageName = db.recentQueriesLocalStorageName;
		localStorage.setObject(localStorageName, []);
	}

	private static getRecentQueriesFromLocalStorage(localStorageName: string): storedQueryDto[]  {
		var recentQueriesFromLocalStorage: storedQueryDto[] = null;
		try {
			recentQueriesFromLocalStorage = localStorage.getObject(localStorageName);
		} catch(err) {
			//no need to do anything
		}
		return recentQueriesFromLocalStorage;
	}
}

export = recentQueriesStorage;