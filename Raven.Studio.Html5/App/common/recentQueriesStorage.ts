/// <reference path="../../Scripts/typings/knockout/knockout.d.ts" />

import database = require("models/resources/database");

class recentQueriesStorage {
    public static getRecentQueries(db: database): storedQueryDto[] {
        var localStorageName = db.recentQueriesLocalStorageName;
        var recentQueriesFromLocalStorage: storedQueryDto[] = localStorage.getObject(localStorageName);
        var isArray = recentQueriesFromLocalStorage instanceof Array;

        if (recentQueriesFromLocalStorage == null || isArray == false) {
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
        var recentQueriesFromLocalStorage: storedQueryDto[] = localStorage.getObject(localStorageName);
        var newRecentQueries = recentQueriesFromLocalStorage.filter((query: storedQueryDto) => query.IndexName != indexName);
        localStorage.setObject(localStorageName, newRecentQueries);
    }
}

export = recentQueriesStorage;