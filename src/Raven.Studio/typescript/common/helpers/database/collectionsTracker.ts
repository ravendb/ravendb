/// <reference path="../../../../typings/tsd.d.ts" />
import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");

class collectionsTracker {

    collections = ko.observableArray<collection>();
    currentCollection = ko.observable<collection>();
    private db: database;
    private resultEtagProvider: () => string;

    constructor(db: database, resultEtagProvider: () => string) {
        this.db = db;
        this.resultEtagProvider = resultEtagProvider;
    }

    dirtyCurrentCollection = ko.observable<boolean>(false);

    onDatabaseStatsChanged(notification: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged) {
        const removedCollections = notification.ModifiedCollections.filter(x => x.Count === -1);
        const changedCollections = notification.ModifiedCollections.filter(x => x.Count !== -1);
        const totalCount = notification.CountOfDocuments;

        // update all collections
        const allDocs = this.collections().find(x => x.isAllDocuments);
        allDocs.documentCount(totalCount);

        removedCollections.forEach(c => {
            const toRemove = this.collections().find(x => x.name.toLocaleLowerCase() === c.Name.toLocaleLowerCase());
            this.onCollectionRemoved(toRemove);
        });

        if (this.currentCollection().isAllDocuments) {
            if (this.resultEtagProvider() !== notification.GlobalDocumentsEtag) {
                this.dirtyCurrentCollection(true);
            }
        }

        changedCollections.forEach(c => {
            const existingCollection = this.collections().find(x => x.name.toLowerCase() == c.Name.toLocaleLowerCase());
            if (existingCollection) {
                this.onCollectionChanged(existingCollection, c);
            } else {
                this.onCollectionCreated(c);
            }
        });
    }

    setCurrentAsNotDirty() {
        this.dirtyCurrentCollection(false);
    }

    getCollectionNames() {
        return this.collections()
            .filter(x => !x.isAllDocuments && !x.isSystemDocuments)
            .map(x => x.name);
    }

    getAllDocumentsCollection() {
        return this.collections().find(x => x.isAllDocuments);
    }

    private onCollectionCreated(incomingItem: Raven.Server.NotificationCenter.Notifications.ModifiedCollection) {
        //TODO: animate incoming db in future

        const newCollection = new collection(incomingItem.Name, this.db, incomingItem.Count);
        const insertIndex = _.sortedIndexBy<collection>(this.collections().slice(1), newCollection, x => x.name.toLocaleLowerCase()) + 1;
        this.collections.splice(insertIndex, 0, newCollection);

        if (this.currentCollection().isAllDocuments) {
            this.dirtyCurrentCollection(true);
        }
    }

    private onCollectionRemoved(item: collection) {
        //TODO: animate removed collection in future

        if (item === this.currentCollection()) {
            // removed current collection - go to all docs
            messagePublisher.reportWarning(item.name + " was removed");
            this.currentCollection(this.getAllDocumentsCollection());
        } else if (this.currentCollection().isAllDocuments) {
            this.dirtyCurrentCollection(true);
        }

        this.collections.remove(item);
    }

    private onCollectionChanged(item: collection, incomingData: Raven.Server.NotificationCenter.Notifications.ModifiedCollection) {
        item.documentCount(incomingData.Count);

        if (item.name === this.currentCollection().name) {
            if (incomingData.CollectionEtag !== this.resultEtagProvider()) {
                this.dirtyCurrentCollection(true);
            }
        }
    }
    
}

export = collectionsTracker;
