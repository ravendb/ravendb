/// <reference path="../../../../typings/tsd.d.ts" />
import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");

class collectionsTracker {

    static default = new collectionsTracker();

    loadStatsTask: JQueryPromise<collectionsStats>;

    collections = ko.observableArray<collection>();

    revisionsBin = ko.observable<collection>();

    private db: database;

    private events = {
        created: [] as Array<(coll: collection) => void>,
        changed: [] as Array<(coll: collection, etag: string) => void>,
        removed: [] as Array<(coll: collection) => void>,
        globalEtag: [] as Array<(etag: string) => void>
    }

    onDatabaseChanged(db: database) {
        this.db = db;
        this.loadStatsTask = new getCollectionsStatsCommand(db)
            .execute()
            .done(stats => this.collectionsLoaded(stats, db));

        this.configureRevisions(db.hasRevisionsConfiguration(), db);

        return this.loadStatsTask;
    }

    configureRevisions(hasRevisionsEnabled: boolean, db: database) {
        if (hasRevisionsEnabled) {
            this.revisionsBin(new collection(collection.revisionsBinCollectionName, db));
        } else {
            this.revisionsBin(null);
        }
    }

    private collectionsLoaded(collectionsStats: collectionsStats, db: database) {
        let collections = collectionsStats.collections;
        collections = _.sortBy(collections, x => x.name.toLocaleLowerCase());

        //TODO: starred
        const allDocsCollection = collection.createAllDocumentsCollection(db, collectionsStats.numberOfDocuments());
        this.collections([allDocsCollection].concat(collections));
    }

    onDatabaseStatsChanged(notification: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged, db: database) {
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

        //TODO this.events.globalEtag.forEach(handler => handler(notification.GlobalDocumentsEtag));

        changedCollections.forEach(c => {
            const existingCollection = this.collections().find(x => x.name.toLowerCase() === c.Name.toLocaleLowerCase());
            if (existingCollection) {
                this.onCollectionChanged(existingCollection, c);
            } else {
                this.onCollectionCreated(c, db);
            }
        });
    }

    getCollectionNames() {
        return this.collections()
            .filter(x => !x.isAllDocuments && !x.isSystemDocuments)
            .map(x => x.name);
    }

    getRevisionsBinCollection() {
        return this.revisionsBin();
    }

    getAllDocumentsCollection() {
        return this.collections().find(x => x.isAllDocuments);
    }

    getSystemDocumentsCollection() {
        return this.collections().find(x => x.isSystemDocuments);
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

    registerOnCollectionUpdatedHandler(handler: (collection: collection, etag: string) => void): disposable {
        this.events.changed.push(handler);

        return {
            dispose: () => _.pull(this.events.changed, handler)
        }
    }

    registerOnGlobalEtagUpdatedHandler(handler: (etag: string) => void): disposable {
        this.events.globalEtag.push(handler);

        return {
            dispose: () => _.pull(this.events.globalEtag, handler)    
        }
    }

    private onCollectionCreated(incomingItem: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged.ModifiedCollection, db: database) {
        const newCollection = new collection(incomingItem.Name, db, incomingItem.Count);
        const insertIndex = _.sortedIndexBy<collection>(this.collections().slice(1), newCollection, x => x.name.toLocaleLowerCase()) + 1;
        this.collections.splice(insertIndex, 0, newCollection);

        this.events.created.forEach(handler => handler(newCollection));
    }

    private onCollectionRemoved(item: collection) {
        this.collections.remove(item);
        this.events.removed.forEach(handler => handler(item));
    }

    private onCollectionChanged(item: collection, incomingData: Raven.Server.NotificationCenter.Notifications.DatabaseStatsChanged.ModifiedCollection) {
        item.documentCount(incomingData.Count);

        //TODO this.events.changed.forEach(handler => handler(item, incomingData.CollectionEtag));
    }
    
}

export = collectionsTracker;
