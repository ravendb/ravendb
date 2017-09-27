/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class recentQueriesStorage {

    static getRecentQueriesWithIndexNameCheck(db: database): JQueryPromise<storedQueryDto[]> {
        const recentQueries = this.getRecentQueries(db);
        return $.when(recentQueries);
    }

    static getRecentQueries(db: database): storedQueryDto[] {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        let recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);

        if (recentQueriesFromLocalStorage == null || recentQueriesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            recentQueriesFromLocalStorage = [];
        }

        return recentQueriesFromLocalStorage;
    }

    static getLastQuery(db: database): string {
        const localStorageName = recentQueriesStorage.getLocalStorageKeyForLastQuery(db.name);
        return this.getLastQueryFromLocalStorage(localStorageName);
    }

    static saveRecentQueries(db: database, recentQueries: storedQueryDto[]) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, recentQueries);
    }

    static saveLastQuery(db: database, lastQuery: string) {
        const localStorageName = recentQueriesStorage.getLocalStorageKeyForLastQuery(db.name);
        localStorage.setObject(localStorageName, lastQuery);
    }

    static removeRecentQueryByQueryText(db: database, queryText: string) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        const recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);
        if (recentQueriesFromLocalStorage == null)
            return;

        const newRecentQueries = recentQueriesFromLocalStorage.filter((query: storedQueryDto) => query.queryText !== queryText);
        localStorage.setObject(localStorageName, newRecentQueries);
    }

    static removeRecentQueries(db: database) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, []);
    }

    private static getLocalStorageKey(dbName: string) {
        return storageKeyProvider.storageKeyFor("recentQueries." + dbName);
    }

    private static getLocalStorageKeyForLastQuery(dbName: string) {
        return storageKeyProvider.storageKeyFor("lastQuery." + dbName);
    }

    private static getRecentQueriesFromLocalStorage(localStorageName: string): storedQueryDto[]  {
        let recentQueriesFromLocalStorage: storedQueryDto[] = null;
        try {
            recentQueriesFromLocalStorage = localStorage.getObject(localStorageName);
        } catch(err) {
            //no need to do anything
        }
        return recentQueriesFromLocalStorage;
    }

    private static getLastQueryFromLocalStorage(localStorageName: string): string {
        let lastQueriesFromLocalStorage: string = null;
        try {
            lastQueriesFromLocalStorage = localStorage.getObject(localStorageName);
        } catch (err) {
            //no need to do anything
        }
        return lastQueriesFromLocalStorage;
    }

    static appendQuery(query: storedQueryDto, recentQueries: KnockoutObservableArray<storedQueryDto>): void {
        const existing = recentQueries().find(q => q.hash === query.hash);
        if (existing) {
            recentQueries.remove(existing);
            recentQueries.unshift(existing);
        } else {
            recentQueries.unshift(query);
        }

        // Limit us to 15 query recent runs.
        if (recentQueries().length > 15) {
            recentQueries.pop();
        }
    }

    static onDatabaseDeleted(qualifer: string, name: string) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(name);
        localStorage.removeItem(localStorageName);

        const localStorageLastQueryName = recentQueriesStorage.getLocalStorageKeyForLastQuery(name);
        localStorage.removeItem(localStorageLastQueryName);
    }

}

export = recentQueriesStorage;
