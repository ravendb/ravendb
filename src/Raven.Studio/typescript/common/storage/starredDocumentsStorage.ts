/// <reference path="../../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import storageKeyProvider = require("common/storage/storageKeyProvider");

class starredDocumentsStorage {

    private static getStarredDocuments(db: database): Array<string> {
        const localStorageName = starredDocumentsStorage.getLocalStorageKey(db.name);
        let starredDocumentsFromLocalStorage: Array<string> = this.getFromLocalStorage(localStorageName);

        if (starredDocumentsFromLocalStorage == null || starredDocumentsFromLocalStorage instanceof Array === false) {
            starredDocumentsFromLocalStorage = [];
            starredDocumentsStorage.saveToLocalStorage(db, starredDocumentsFromLocalStorage);
        }

        return starredDocumentsFromLocalStorage;
    }

    static getStarredDocumentsWithDocumentIdsCheck(db: database): JQueryPromise<Array<string>> {
        const starred = starredDocumentsStorage.getStarredDocuments(db);
        return new verifyDocumentsIDsCommand(starred, db)
            .execute()
            .done((verifiedIds) => {
                const invalidIds = starred.filter(x => !verifiedIds.includes(x));
                if (invalidIds.length) {
                    starredDocumentsStorage.saveToLocalStorage(db, verifiedIds);
                }
            });
    }

    static isStarred(db: database, documentId: string): boolean {
        const starred = starredDocumentsStorage.getStarredDocuments(db);
        return _.includes(starred, documentId);
    }

    static markDocument(db: database, documentId: string, asStarred: boolean) {
        const starred = starredDocumentsStorage.getStarredDocuments(db);
        const alreadyStored = _.includes(starred, documentId); 
        if (asStarred) {
            if (!alreadyStored) {
                const locationToInsert = _.sortedIndex(starred, documentId);
                starred.splice(locationToInsert, 0, documentId);
                starredDocumentsStorage.saveToLocalStorage(db, starred);
            }
        } else {
            if (alreadyStored) {
                _.pull(starred, documentId);
                starredDocumentsStorage.saveToLocalStorage(db, starred);
            }
        }
    }

    private static saveToLocalStorage(db: database, starredDocuments: string[]) {
        const localStorageName = starredDocumentsStorage.getLocalStorageKey(db.name);
        localStorage.setObject(localStorageName, starredDocuments);
    }

    private static getLocalStorageKey(dbName: string) {
        return storageKeyProvider.storageKeyFor("starredDocuments." + dbName);
    }

    private static getFromLocalStorage(localStorageName: string): string[]  {
        let starredDocumentsFromLocalStorage: string[] = null;
        try {
            starredDocumentsFromLocalStorage = localStorage.getObject(localStorageName);
        } catch(err) {
            //no need to do anything
        }
        return starredDocumentsFromLocalStorage;
    }

    static onDatabaseDeleted(qualifer: string, name: string) {
        const localStorageName = starredDocumentsStorage.getLocalStorageKey(name);
        localStorage.removeItem(localStorageName);
    }
}

export = starredDocumentsStorage;
