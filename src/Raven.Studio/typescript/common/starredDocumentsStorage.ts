/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");

class starredDocumentsStorage {
    static getStarredDocuments(db: database): Array<string> {
        const localStorageName = db.starredDocumentsLocalStorageName;
        let starredDocumentsFromLocalStorage: Array<string> = this.getFromLocalStorage(localStorageName);

        if (starredDocumentsFromLocalStorage == null || starredDocumentsFromLocalStorage instanceof Array === false) {
            starredDocumentsFromLocalStorage = [];
            starredDocumentsStorage.saveToLocalStorage(db, starredDocumentsFromLocalStorage);
        }

        return starredDocumentsFromLocalStorage;
    }

    static isStarred(db: database, documentId: string): boolean {
        let starred = starredDocumentsStorage.getStarredDocuments(db);
        return starred.contains(documentId);
    }

    static markDocument(db: database, documentId: string, asStarred: boolean) {
        let starred = starredDocumentsStorage.getStarredDocuments(db);
        let alreadyStored = starred.contains(documentId); 
        if (asStarred) {
            if (!alreadyStored) {
                starred.unshift(documentId);
                starredDocumentsStorage.saveToLocalStorage(db, starred);
            }
        } else {
            if (alreadyStored) {
                starred.remove(documentId);
                starredDocumentsStorage.saveToLocalStorage(db, starred);
            }
        }
    }

    private static saveToLocalStorage(db: database, starredDocuments: string[]) {
        var localStorageName = db.starredDocumentsLocalStorageName;
        localStorage.setObject(localStorageName, starredDocuments);
    }

    private static getFromLocalStorage(localStorageName: string): string[]  {
        var starredDocumentsFromLocalStorage: string[] = null;
        try {
            starredDocumentsFromLocalStorage = localStorage.getObject(localStorageName);
        } catch(err) {
            //no need to do anything
        }
        return starredDocumentsFromLocalStorage;
    }
}

export = starredDocumentsStorage;
