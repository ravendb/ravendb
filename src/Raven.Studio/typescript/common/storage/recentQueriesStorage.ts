/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");

class recentQueriesStorage {
    static getRecentQueries(db: database): storedQueryDto[] {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        let recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);

        if (recentQueriesFromLocalStorage == null || recentQueriesFromLocalStorage instanceof Array === false) {
            localStorage.setObject(localStorageName, []);
            recentQueriesFromLocalStorage = [];
        }

        // we have restore properties coming from local storage, 
        // when saving object.prop = undefined in localstorage 
        // we don't get 'prop' property during read.
        recentQueriesFromLocalStorage.forEach(entry => {
            if (entry.TransformerQuery) {
                if (("transformerName" in entry.TransformerQuery) === false) {
                    entry.TransformerQuery.transformerName = undefined;
                };
            }
        });

        return recentQueriesFromLocalStorage;
    }

    static saveRecentQueries(db: database, recentQueries: storedQueryDto[]) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, recentQueries);
    }

    static removeIndexFromRecentQueries(db: database, indexName: string) {
        recentQueriesStorage.removeIndexFromRecentQueriesByName(db.name, indexName);
    }

    private static removeIndexFromRecentQueriesByName(dbName: string, indexName: string) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(dbName);
        const recentQueriesFromLocalStorage: storedQueryDto[] = this.getRecentQueriesFromLocalStorage(localStorageName);
        if (recentQueriesFromLocalStorage == null)
            return;

        const newRecentQueries = recentQueriesFromLocalStorage.filter((query: storedQueryDto) => query.IndexName != indexName);
        localStorage.setObject(localStorageName, newRecentQueries);
    }

    static removeRecentQueries(db: database) {
        const localStorageName = recentQueriesStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, []);
    }

    private static getLocalStorageKey(dbName: string) {
        return "ravenDB-recentQueries." + dbName;
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

    static onResourceDeleted(qualifer: string, name: string) {
        if (qualifer === database.qualifier) {
            const localStorageName = recentQueriesStorage.getLocalStorageKey(name);
            localStorage.removeItem(localStorageName);
        }
    }

    static onIndexDeleted(dbName: string, indexName: string) {
        recentQueriesStorage.removeIndexFromRecentQueriesByName(dbName, indexName);
    }


}

export = recentQueriesStorage;
