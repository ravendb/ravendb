import app = require("durandal/app");
import router = require("plugins/router");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import deleteCollection = require("viewmodels/deleteCollection");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import getCustomColumnsCommand = require('commands/getCustomColumnsCommand');
import viewModelBase = require("viewmodels/viewModelBase");
import virtualTable = require("widgets/virtualTable/viewModel");
import customColumnParams = require('models/customColumnParams');
import customColumns = require('models/customColumns');
import selectColumns = require('viewmodels/selectColumns');

class documents extends viewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
    collectionToSelectName: string;
    currentCollectionPagedItems = ko.observable<pagedList>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    selectedDocumentIndices = ko.observableArray<number>();
    isSelectAll = ko.observable(false);
    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    contextName = ko.observable<string>('');
    currentCollection = ko.observable <collection>();

    static gridSelector = "#documentsGrid";

    constructor() {
        super();
        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
        this.hasAnyDocumentsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);
    }

    activate(args) {
        super.activate(args);

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;
        this.fetchCollections(appUrl.getDatabase());
    }

    attached() {
        // Initialize the context menu (using Bootstrap-ContextMenu library).
        // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
        (<any>$('.document-collections')).contextmenu({
            target: '#collections-context-menu'
        });
    }


    collectionsLoaded(collections: Array<collection>, db: database) {
        
        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = collection.createAllDocsCollection(db);
        this.allDocumentsCollection.documentCount = ko.computed(() =>
            this.collections()
                .filter(c => c !== this.allDocumentsCollection) // Don't include self, the all documents collection.
                .map(c => c.documentCount()) // Grab the document count of each.
                .reduce((first: number, second: number) => first + second, 0)); // And sum them up.

        // Create the "System Documents" pseudo collection.
        var systemDocumentsCollection = collection.createSystemDocsCollection(db);

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

    //TODO: this binding has notification leak!
    selectedCollectionChanged(selected: collection) {
        if (selected) {

            var customColumnsCommand = selected.isAllDocuments ?
                getCustomColumnsCommand.forAllDocuments(this.activeDatabase()) : getCustomColumnsCommand.forCollection(selected.name, this.activeDatabase());

            this.contextName(customColumnsCommand.docName);

            customColumnsCommand.execute().done((dto: customColumnsDto) => {
                if (dto) {
                    this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                    this.currentColumnsParams().customMode(true);
                } else {
                    // use default values!
                    this.currentColumnsParams().columns.removeAll();
                    this.currentColumnsParams().customMode(false);
                }

                var pagedList = selected.getDocuments();
                this.currentCollectionPagedItems(pagedList);
                this.currentCollection(selected);
            });
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

    selectColumns() {
        var selectColumnsViewModel: selectColumns = new selectColumns(this.currentColumnsParams().clone(), this.contextName(), this.activeDatabase());
        app.showDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols) => {
            this.currentColumnsParams(cols);

            var pagedList = this.currentCollection().getDocuments();
            this.currentCollectionPagedItems(pagedList);
        });
    }

    fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db)
            .execute()
            .done(results => this.collectionsLoaded(results, db));
    }

    newDocument() {
        router.navigate(appUrl.forNewDoc(this.activeDatabase()));
    }

    toggleSelectAll() {
        this.isSelectAll.toggle();

        var docsGrid = this.getDocumentsGrid();
        if (docsGrid && this.isSelectAll()) {
            docsGrid.selectAll();
        } else if (docsGrid && !this.isSelectAll()) {
            docsGrid.selectNone();
        }        
    }

    editSelectedDoc() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    deleteSelectedDocs() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.deleteSelectedItems();
        }
    }

    copySelectedDocs() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocs();
        }
    }

    copySelectedDocIds() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocIds();
        }
    }

    getDocumentsGrid(): virtualTable {
        var gridContents = $(documents.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = documents;