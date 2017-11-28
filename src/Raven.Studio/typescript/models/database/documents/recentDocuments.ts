import database = require("models/resources/database");
import appUrl = require("common/appUrl");

class recentDocuments {
    private static recentDocumentsInDatabases = ko.observableArray<{ databaseName: string; recentDocuments: KnockoutObservableArray<string> }>();
    static maxRecentItems = 20;
    
    static getTopRecentDocumentsAsObservable(db: database): KnockoutObservableArray<string> {
        const currentDbName = db ? db.name : null;
        let recentDocumentsForCurDb = recentDocuments.recentDocumentsInDatabases()
            .find(x => x.databaseName === currentDbName);
        
        if (!recentDocumentsForCurDb) {
            recentDocumentsForCurDb = { databaseName: db.name, recentDocuments: ko.observableArray<string>([]) };
            recentDocuments.recentDocumentsInDatabases.push(recentDocumentsForCurDb);
        }
        
        return recentDocumentsForCurDb.recentDocuments;
    }

    getTopRecentDocuments(activeDatabase: database, documentId: string): Array<connectedDocument> {
        const currentDbName = activeDatabase ? activeDatabase.name : null;
        const recentDocumentsForCurDb = recentDocuments.recentDocumentsInDatabases()
            .find(x => x.databaseName === currentDbName);
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
            .find(x => x.databaseName === db.name);
        if (recentDocsForDatabase) {
            recentDocsForDatabase.recentDocuments.remove(docId);
        }
    }

    appendRecentDocument(db: database, docId: string): void {
        if (!docId) {
            return;
        }
        
        const existingRecentDocumentsStore = recentDocuments.recentDocumentsInDatabases().find(x => x.databaseName === db.name);
        if (existingRecentDocumentsStore) {
            const recentDocs = existingRecentDocumentsStore.recentDocuments();
            _.pull(recentDocs, docId);
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
            .find(x => x.databaseName === db.name);
        if (recentDocsForDatabase && recentDocsForDatabase.recentDocuments().length) {
            return recentDocsForDatabase.recentDocuments()[0];
        }
        return null;
    }
}

export = recentDocuments;
