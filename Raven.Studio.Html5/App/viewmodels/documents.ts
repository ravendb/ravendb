import app = require("durandal/app");
import router = require("plugins/router");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import activeDbViewModelBase = require("viewmodels/activeDbViewModelBase");

class documents extends activeDbViewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
    collectionToSelectName: string;
    private currentCollectionPagedItems = ko.observable<pagedList>();

    constructor() {
        super();
        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
    }

    activate(args) {
        super.activate(args);
        this.activeDatabase.subscribe((db: database) => this.databaseChanged(db));

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;
        
        return this.fetchCollections(appUrl.getDatabase());
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

        var collectionToSelect = allCollections.first(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection;
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
            var documentsUrl = appUrl.forDocuments(null, db);
            router.navigate(documentsUrl, false);
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

    selectCollection(collection: collection) {
        collection.activate();
        var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
        router.navigate(documentsWithCollectionUrl, false);
    }

    fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db)
            .execute()
            .done(results => this.collectionsLoaded(results, db));
    }
}

export = documents;