import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class recentDocuments {
    //TODO: consider using es6 map
    private static recentDocumentsInDatabases = ko.observableArray<{ databaseName: string; recentDocuments: KnockoutObservableArray<string> }>();
    static maxRecentItems = 20;

    getTopRecentDocuments(activeDatabase: database, documentId: string): Array<connectedDocument> {
        const currentDbName = activeDatabase ? activeDatabase.name : null;
        const recentDocumentsForCurDb = recentDocuments.recentDocumentsInDatabases()
            .first(x => x.databaseName === currentDbName);
        if (recentDocumentsForCurDb) {
            const value = recentDocumentsForCurDb
                .recentDocuments()
                .filter((x: string) => x !== documentId)
                .slice(0, recentDocuments.maxRecentItems)
                .map((docId: string) => ({
                        id: docId,
                        href: appUrl.forEditDoc(docId, activeDatabase)
                    }) as connectedDocument);
            return value;
        } else {
            return [];
        }
    }

    documentRemoved(db: database, docId: string) {
        const recentDocsForDatabase = recentDocuments.recentDocumentsInDatabases()
            .first(x => x.databaseName === db.name);
        if (recentDocsForDatabase) {
            recentDocsForDatabase.recentDocuments.remove(docId);
        }
    }

    appendRecentDocument(db: database, docId: string): void {
        var existingRecentDocumentsStore = recentDocuments.recentDocumentsInDatabases.first(x => x.databaseName === db.name);
        if (existingRecentDocumentsStore) {
            const recentDocs = existingRecentDocumentsStore.recentDocuments();
            recentDocs.remove(docId);
            recentDocs.unshift(docId);
            while (recentDocs.length > recentDocuments.maxRecentItems) {
                recentDocs.pop();
            }
            existingRecentDocumentsStore.recentDocuments(recentDocs);
        } else {
            recentDocuments.recentDocumentsInDatabases.push({ databaseName: db.name, recentDocuments: ko.observableArray<string>([docId]) });
        }
    }

    getPreviousDocument(db: database): string {
        const recentDocsForDatabase = recentDocuments.recentDocumentsInDatabases()
            .first(x => x.databaseName === db.name);
        if (recentDocsForDatabase && recentDocsForDatabase.recentDocuments().length) {
            return recentDocsForDatabase.recentDocuments()[0];
        }
        return null;
    }
}

export = recentDocuments;