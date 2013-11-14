import http = require("plugins/http");
import app = require("durandal/app");
import sys = require("durandal/system");
import router = require("plugins/router");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import raven = require("common/raven");
import pagedList = require("common/pagedList");

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

        var dbChangedSubscription = ko.postbox.subscribe("ActivateDatabase", db => this.databaseChanged(db));
        this.subscriptions.push(dbChangedSubscription);

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo/123&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        // See if we've got a database to select.
        if (args && args.database) {
            ko.postbox.publish("ActivateDatabaseWithName", args.database);
        }

        return this.fetchCollections();
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

    collectionsLoaded(collections: Array<collection>) {
        // Set the color class for each of the collections.
        // These styles are found in app.less.
        var collectionStyleCount = 15;
        collections.forEach((c, index) => c.colorClass = "collection-style-" + (index % collectionStyleCount));

        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = new collection("All Documents", true);
        this.allDocumentsCollection.colorClass = "all-documents-collection";
        <any>this.allDocumentsCollection.documentCount = ko.computed(() =>
            this.collections()
                .filter(c => c !== this.allDocumentsCollection) // Don't include self, the all documents collection.
                .map(c => c.documentCount()) // Grab the document count of each.
                .reduce((first, second) => first + second, 0)); // And sum them up.

        // All systems a-go. Load them into the UI and select the first one.
        var allCollections = [this.allDocumentsCollection].concat(collections);
        this.collections(allCollections);

        var collectionToSelect = collections.filter(c => c.name === this.collectionToSelectName)[0] || this.allDocumentsCollection;
        collectionToSelect.activate();

        // Fetch the collection info for each collection.
        // The collection info contains information such as total number of documents.
        collections.forEach(c => this.fetchTotalDocuments(c));
    }

    fetchTotalDocuments(collection: collection) {
        this.ravenDb
            .collectionInfo(collection.name)
            .done(info => {
                collection.documentCount(info.totalResults);
            });
    }

    selectedCollectionChanged(selected: collection) {
        if (collection) {
            var fetcher = (skip: number, take: number) => {
                var collectionName = selected !== this.allDocumentsCollection ? selected.name : null;
                return this.ravenDb.documents(collectionName, skip, take);
            };

            var documentsList = new pagedList(fetcher);
            documentsList.collectionName = selected.name;
			this.currentCollectionPagedItems(documentsList);
        }
    }

    databaseChanged(db: database) {
        if (db) {
            router.navigate("#documents?database=" + encodeURIComponent(db.name), false);
            this.fetchCollections();
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

    fetchCollections(): JQueryPromise<Array<collection>> {
        return this.ravenDb
            .collections()
            .done(results => this.collectionsLoaded(results));
    }
}

export = documents;