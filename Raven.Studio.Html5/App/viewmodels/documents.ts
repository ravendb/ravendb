import app = require("durandal/app");
import router = require("plugins/router");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import raven = require("common/raven");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getDocumentsCommand = require("commands/getDocumentsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");

class documents {

    displayName = "documents";
    ravenDb: raven;
    collections = ko.observableArray<collection>();
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
    collectionColors = [];
    collectionToSelectName: string;
    private currentCollectionPagedItems = ko.observable<pagedList>();
    subscriptions: Array<KnockoutSubscription> = [];

    constructor() {
        this.ravenDb = new raven();
        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
    }

    activate(args) {

        var dbChangedSubscription = ko.postbox.subscribe("ActivateDatabase", (db: database) => this.databaseChanged(db));
        this.subscriptions.push(dbChangedSubscription);

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        // See if we've got a database to select.
        if (args && args.database) {
            ko.postbox.publish("ActivateDatabaseWithName", args.database);
        }

        return this.fetchCollections(appUrl.getDatabase());
    }

    deactivate() {
        // Unsubscribe when we leave the page.
        // This is necessary, otherwise our subscriptions will keep the page alive in memory and otherwise screw with us.
        this.subscriptions.forEach(s => s.dispose());
    }

    attached(view: HTMLElement, parent: HTMLElement) {
        // Initialize the context menu (using Bootstrap-ContextMenu library).
        // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
        (<any>$('.document-collections li')).contextmenu({
            target: '#collections-context-menu'
        });
    }

    collectionsLoaded(collections: Array<collection>, db: database) {

        // Set the color class for each of the collections.
        // These styles are found in app.less.
        var collectionStyleCount = 15;
        collections.forEach((c, index) => c.colorClass = "collection-style-" + (index % collectionStyleCount));

        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = collection.createAllDocsCollection(db);
        this.allDocumentsCollection.colorClass = "all-documents-collection";
        this.allDocumentsCollection.documentCount = ko.computed(() =>
            this.collections()
                .filter(c => c !== this.allDocumentsCollection) // Don't include self, the all documents collection.
                .map(c => c.documentCount()) // Grab the document count of each.
                .reduce((first: number, second: number) => first + second, 0)); // And sum them up.

        // Create the "System Documents" pseudo collection.
        var systemDocumentsCollection = collection.createSystemDocsCollection(db);
        systemDocumentsCollection.colorClass = "system-documents-collection";

        // All systems a-go. Load them into the UI and select the first one.
        var collectionsWithSysCollection = [systemDocumentsCollection].concat(collections);
        var allCollections = [this.allDocumentsCollection].concat(collectionsWithSysCollection);
        this.collections(allCollections);

        var collectionToSelect = allCollections.first<collection>(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection;
        collectionToSelect.activate();

        // Fetch the collection info for each collection.
        // The collection info contains information such as total number of documents.
        collectionsWithSysCollection.forEach(c => c.fetchTotalDocumentCount());
    }

    selectedCollectionChanged(selected: collection) {
        if (selected) {
            var pagedList = selected.getDocuments();
			this.currentCollectionPagedItems(pagedList);
        }
    }

    databaseChanged(db: database) {
        if (db) {
            // TODO: use appUrl here.
            router.navigate("#documents?database=" + encodeURIComponent(db.name), false);
            this.fetchCollections(db);
        }
    }

    deleteCollection() {
        var collection = this.selectedCollection();
        if (collection) {
            var viewModel = new deleteCollection(collection);
            viewModel.deletionTask.done(() => {
                this.collections.remove(collection);
                this.allDocumentsCollection.activate();
            });
            app.showDialog(viewModel);
        }
    }

    activateCollection(collection: collection) {
        collection.activate();
        var collectionPart = "collection=" + encodeURIComponent(collection.name);
        var databasePart = raven.activeDatabase() ? "&database=" + raven.activeDatabase().name : "";
        router.navigate("#documents?" + collectionPart + databasePart, false);
    }

    fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db)
            .execute()
            .done(results => this.collectionsLoaded(results, db));
    }
}

export = documents;