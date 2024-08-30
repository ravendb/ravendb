/// <reference path="../../../../typings/tsd.d.ts" />
import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import generalUtils = require("common/generalUtils");

class collectionsTracker {

    static default = new collectionsTracker();

    loadStatsTask: JQueryPromise<collectionsStats>;

    collections = ko.observableArray<collection>();

    revisionsBin = ko.observable<collection>();

    conflictsCount = ko.observable<number>();

    private db: database;

    private events = {
        created: [] as Array<(coll: collection) => void>,
        changed: [] as Array<(coll: collection, changeVector: string) => void>,
        removed: [] as Array<(coll: collection) => void>,
        globalChangeVector: [] as Array<(changeVector: string) => void>
    };

    onDatabaseChanged(db: database) {
        this.db = db;
        
        this.loadStatsTask = new getCollectionsStatsCommand(db)
            .execute()
            .done(stats => this.collectionsLoaded(stats));

        this.configureRevisions(db);

        return this.loadStatsTask;
    }

    configureRevisions(db: database) {
        if (db.hasRevisionsConfiguration()) {
            this.revisionsBin(new collection(collection.revisionsBinCollectionName));
        } else {
            this.revisionsBin(null);
        }
    }

    private collectionsLoaded(collectionsStats: collectionsStats) {
        const collections = collectionsStats.collections.filter(x => x.documentCount());
        
        collections.sort((a, b) => this.sortAlphaNumericCollection(a.name, b.name));

        const allDocsCollection = collection.createAllDocumentsCollection(collectionsStats.numberOfDocuments());
        this.collections([allDocsCollection].concat(collections));

        this.conflictsCount(collectionsStats.numberOfConflicts);
    }

    getCollectionCount(collectionName: string) {
        if (collectionName === collection.allDocumentsCollectionName) {
            return this.getAllDocumentsCollection().documentCount();
        }
        
        const matchedCollection = this.collections().find(x => x.name === collectionName);
        
        return matchedCollection ? matchedCollection.documentCount() : 0;
    }
    
    getCollectionColorIndex(collectionName: string) {
        return (collectionsTracker.default.collections().findIndex( x => x.name === collectionName) + 5) % 6;
        // 6 is the number of classes that I have defined in etl.less for colors...
    }    
    
    onDatabaseStatsChanged(notification: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        const removedCollections = notification.ModifiedCollections.filter(x => x.Count < 1);
        const changedCollections = notification.ModifiedCollections.filter(x => x.Count >= 1);
        const totalCount = notification.CountOfDocuments;

        // update all collections
        const allDocs = this.collections().find(x => x.isAllDocuments);
        allDocs.documentCount(totalCount);

        removedCollections.forEach(c => {
            const toRemove = this.collections().find(x => x.name.toLocaleLowerCase() === c.Name.toLocaleLowerCase());
            if (toRemove) {
                this.onCollectionRemoved(toRemove);    
            }
        });

        this.events.globalChangeVector.forEach(handler => handler(notification.GlobalChangeVector));

        changedCollections.forEach(c => {
            const existingCollection = this.collections().find(x => x.name.toLowerCase() === c.Name.toLocaleLowerCase());
            if (existingCollection) {
                this.onCollectionChanged(existingCollection, c);
            } else {
                this.onCollectionCreated(c);
            }
        });
        
        this.conflictsCount(notification.CountOfConflicts);
    }

    getCollectionNames() {
        return this.collections()
            .filter(x => !x.isAllDocuments)
            .map(x => x.name);
    }

    getRevisionsBinCollection() {
        return this.revisionsBin();
    }

    getAllDocumentsCollection() {
        return this.collections().find(x => x.isAllDocuments);
    }

    registerOnCollectionCreatedHandler(handler: (collection: collection) => void): disposable {
        this.events.created.push(handler);

        return {
            dispose: () => _.pull(this.events.created, handler)
        }
    }

    registerOnCollectionRemovedHandler(handler: (collection: collection) => void): disposable {
        this.events.removed.push(handler);

        return {
            dispose: () => _.pull(this.events.removed, handler)
        }
    }

    registerOnCollectionUpdatedHandler(handler: (collection: collection, lastDocumentChangeVector: string) => void): disposable { 
        this.events.changed.push(handler);

        return {
            dispose: () => _.pull(this.events.changed, handler)
        }
    }

    registerOnGlobalChangeVectorUpdatedHandler(handler: (changeVector: string) => void): disposable {
        this.events.globalChangeVector.push(handler);

        return {
            dispose: () => _.pull(this.events.globalChangeVector, handler)    
        }
    }

    private onCollectionCreated(incomingItem: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged.ModifiedCollection) {
        const newCollection = new collection(incomingItem.Name, incomingItem.Count);
        this.collections.push(newCollection);
        this.collections.sort((a, b) => this.sortAlphaNumericCollection(a.name, b.name));

        this.events.created.forEach(handler => handler(newCollection));
    }

    private sortAlphaNumericCollection(a: string, b: string) {
        if (a.toLowerCase() === "@empty") {
            return -1;
        }

        if (b.toLowerCase() === "@empty") {
            return 1;
        }

        return generalUtils.sortAlphaNumeric(a, b);
    }

    private onCollectionRemoved(item: collection) {
        this.collections.remove(item);
        this.events.removed.forEach(handler => handler(item));
    }

    private onCollectionChanged(item: collection, incomingData: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged.ModifiedCollection) {
        if (item.lastDocumentChangeVector() === incomingData.LastDocumentChangeVector && item.documentCount() === incomingData.Count) {
            // no change 
            return;
        }
        
        item.documentCount(incomingData.Count);
        item.lastDocumentChangeVector(incomingData.LastDocumentChangeVector);

        this.events.changed.forEach(handler => handler(item, incomingData.LastDocumentChangeVector));

        // If no documents - remove collection on studio side - server doesn't actually have a way to delete collections      
        if (!incomingData.Count) {
            this.onCollectionRemoved(item);
        }
    }
    
}

export = collectionsTracker;
