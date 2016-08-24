import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class recentDocuments {
    static recentDocumentsInDatabases = ko.observableArray<{ databaseName: string; recentDocuments: KnockoutObservableArray<string> }>();

    getTopRecentDocuments(activeDatabase: database, documentId: string) {
        var currentDbName = activeDatabase ? activeDatabase.name : null;
        var recentDocumentsForCurDb = recentDocuments.recentDocumentsInDatabases().first(x => x.databaseName === currentDbName);
        if (recentDocumentsForCurDb) {
            var value = recentDocumentsForCurDb
                .recentDocuments()
                .filter((x: string) => {
                    return x !== documentId;
                })
                .slice(0, 5)
                .map((docId: string) => {
                    return {
                        docId: (docId.length > 35) ? docId.substr(0, 35) + '...' : docId,
                        docUrl: appUrl.forEditDoc(docId, null, null, activeDatabase),
                        fullDocId: docId
                    };
                });
            return value;
        } else {
            return [];
        }
    }

    appendRecentDocument(db: database, docId: string) {
        var existingRecentDocumentsStore = recentDocuments.recentDocumentsInDatabases.first(x => x.databaseName === db.name);
        if (existingRecentDocumentsStore) {
            var existingDocumentInStore = existingRecentDocumentsStore.recentDocuments.first(x => x === docId);
            if (!existingDocumentInStore) {
                if (existingRecentDocumentsStore.recentDocuments().length === 5) {
                    existingRecentDocumentsStore.recentDocuments.pop();
                }
                existingRecentDocumentsStore.recentDocuments.unshift(docId);
            }

        } else {
            recentDocuments.recentDocumentsInDatabases.push({ databaseName: db.name, recentDocuments: ko.observableArray([docId]) });
        }

    }
}

export = recentDocuments;