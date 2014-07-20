import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");

import shell = require("viewmodels/shell");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteCollection = require("viewmodels/deleteCollection");

import collection = require("models/collection");
import database = require("models/database");
import document = require("models/document");
import changeSubscription = require('models/changeSubscription');
import customFunctions = require("models/customFunctions");
import customColumns = require('models/customColumns');
import customColumnParams = require('models/customColumnParams');

import getCollectionsCommand = require("commands/getCollectionsCommand");
import getCustomColumnsCommand = require('commands/getCustomColumnsCommand');
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");
import getOperationStatusCommand = require('commands/getOperationStatusCommand');

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");

class documents extends viewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
    collectionToSelectName: string;
    currentCollectionPagedItems = ko.observable<pagedList>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentCustomFunctions = ko.observable<customFunctions>(customFunctions.empty());
    selectedDocumentIndices = ko.observableArray<number>();
    hasDocuments: KnockoutComputed<boolean>;
    contextName = ko.observable<string>('');
    currentCollection = ko.observable<collection>();
    showLoadingIndicator = ko.observable<boolean>(false);
    currentExportUrl: KnockoutComputed<string>;
    isRegularCollection: KnockoutComputed<boolean>;

    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    hasAllDocumentsSelected: KnockoutComputed<boolean>;
    isAnyDocumentsAutoSelected = ko.observable<boolean>(false);
    isAllDocumentsAutoSelected = ko.observable<boolean>(false);
    canCopyAllSelected: KnockoutComputed<boolean>;
    rightClickedCollection = ko.observable<collection>();
    isRightClickedCollectionRegular: KnockoutComputed<boolean>;

    static gridSelector = "#documentsGrid";

    constructor() {
        super();

        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
        this.hasDocuments = ko.computed(() => {
            var selectedCollection: collection = this.selectedCollection();
            if (!!selectedCollection) {
                if (selectedCollection.name == collection.allDocsCollectionName) {
                    var db: database = this.activeDatabase();
                    return db.itemCount() > 0;
                }
                return this.selectedCollection().documentCount() > 0;
            }
            return false;
        });
        this.hasAnyDocumentsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);
        this.hasAllDocumentsSelected = ko.computed(() => {
            var numOfSelectedDocuments = this.selectedDocumentIndices().length;
            if (!!this.selectedCollection() && numOfSelectedDocuments != 0) {
                return numOfSelectedDocuments == this.selectedCollection().documentCount();
            }
            return false;
        });
        this.canCopyAllSelected = ko.computed(() => {
            this.showLoadingIndicator(); //triggers computing the new cached selected items
            var numOfSelectedDocuments = this.selectedDocumentIndices().length;
            var docsGrid = this.getDocumentsGrid();

            if (!!docsGrid) {
                var cachedItems = docsGrid.getNumberOfCachedItems();
                return cachedItems >= numOfSelectedDocuments;
            }

            return false;
        });
        this.isRightClickedCollectionRegular = ko.computed(() => {
            var clickedCollection: collection = this.rightClickedCollection();
            return !!clickedCollection && !clickedCollection.isAllDocuments && !clickedCollection.isSystemDocuments;
        });
        this.isRegularCollection = ko.computed(() => {
            var collection: collection = this.selectedCollection();
            return !!collection && !collection.isAllDocuments && !collection.isSystemDocuments;
        });
        this.currentExportUrl = ko.computed(() => {
            var collection: collection = this.selectedCollection();
            if (this.isRegularCollection()) {
                return appUrl.forExportCollectionCsv(collection, collection.ownerDatabase);
            }
            return null;
        });

        ko.postbox.subscribe("ChangesApiReconnected", (db: database) => this.reloadDocumentsData(db));
    }

    activate(args) {
        super.activate(args);

        this.fetchCustomFunctions();

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        var db = this.activeDatabase();
        this.fetchCollections(db).done(results => this.collectionsLoaded(results, db));
    }

    attached() {
        // Initialize the context menu (using Bootstrap-ContextMenu library).
        // TypeScript doesn't know about Bootstrap-Context menu, so we cast jQuery as any.
        (<any>$('.document-collections')).contextmenu({
            target: '#collections-context-menu',
            before: (e: MouseEvent) => {
                this.rightClickedCollection(ko.dataFor(e.target));
                return true;
            }
        });
    }

    private fetchCollections(db: database): JQueryPromise<Array<collection>> {
        return new getCollectionsCommand(db).execute();
    }

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentResourceChangesApi().watchAllIndexes(() => this.refreshCollections()),
            shell.currentResourceChangesApi().watchAllDocs(() => this.refreshCollections()),
            shell.currentResourceChangesApi().watchBulks(() => this.refreshCollections())
        ];
    }

    private refreshCollections(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();

        this.fetchCollections(db).done(results => {
            this.updateCollections(results, db);
            deferred.resolve();
        });

        return deferred;
    }

    collectionsLoaded(collections: Array<collection>, db: database) {
        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = collection.createAllDocsCollection(db);
        this.allDocumentsCollection.documentCount = ko.computed(() => db.itemCount());

        // Create the "System Documents" pseudo collection.
        var systemDocumentsCollection = collection.createSystemDocsCollection(db);

        // All systems a-go. Load them into the UI and select the first one.
        var collectionsWithSysCollection = [systemDocumentsCollection].concat(collections);
        var allCollections = [this.allDocumentsCollection].concat(collectionsWithSysCollection);
        this.collections(allCollections);

        var collectionToSelect = allCollections.first(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection;
        this.rightClickedCollection(collectionToSelect);
        collectionToSelect.activate();
    }

    fetchCustomFunctions() {
        var customFunctionsCommand = new getCustomFunctionsCommand(this.activeDatabase()).execute();
        customFunctionsCommand.done((cf: customFunctions) => {
            this.currentCustomFunctions(cf);
        });
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

    deleteCollection(collection: collection) {
        if (collection) {
            var viewModel = new deleteCollection(collection);
            viewModel.deletionTask.done((result: operationIdDto) => {
                if (!collection.isAllDocuments) {
                    this.collections.remove(collection);

                    var selectedCollection: collection = this.selectedCollection();
                    if (collection.name == selectedCollection.name) {
                        this.selectCollection(this.allDocumentsCollection);
                    }
                } else {
                    this.selectNone();
                }

                this.updateGridAfterOperationComplete(collection, result.OperationId);
            });
            app.showDialog(viewModel);
        }
    }

    private updateGridAfterOperationComplete(collection: collection, operationId: number) {
        var getOperationStatusTask = new getOperationStatusCommand(collection.ownerDatabase, operationId);
        getOperationStatusTask.execute()
            .done((result: operationStatusDto) => {
                if (result.Completed) {
                    var selectedCollection: collection = this.selectedCollection();

                    if (selectedCollection.isAllDocuments) {
                        var docsGrid = this.getDocumentsGrid();
                        docsGrid.refreshCollectionData();
                    } else {
                        var allDocumentsPagedList = this.allDocumentsCollection.getDocuments();
                        allDocumentsPagedList.invalidateCache();
                    }
                } else {
                    setTimeout(() => this.updateGridAfterOperationComplete(collection, operationId), 500);
                }
            });
    }

    private updateCollections(receivedCollections: Array<collection>, db: database) {
        var deletedCollections = [];

        this.collections().forEach((col: collection) => {
            if (!receivedCollections.first((receivedCol: collection) => col.name == receivedCol.name) && col.name != 'System Documents' && col.name != 'All Documents') {
                deletedCollections.push(col);
            }
        });

        this.collections.removeAll(deletedCollections);

        receivedCollections.forEach((receivedCol: collection) => {
            var foundCollection = this.collections().first((col: collection) => col.name == receivedCol.name);
            if (!foundCollection) {
                this.collections.push(receivedCol);
            } else {
                foundCollection.documentCount(receivedCol.documentCount());
            }
        });

        //if the collection is deleted, go to the all documents collection
        var currentCollection: collection = this.collections().first(c => c.name === this.selectedCollection().name);
        if (!currentCollection || currentCollection.documentCount() == 0) {
            this.selectCollection(this.allDocumentsCollection);
        }
    }

    private reloadDocumentsData(db: database) {
        if (db.name == this.activeDatabase().name) {
            this.refreshCollections().done(() => {
                var selectedCollection: collection = this.selectedCollection();

                this.collections().forEach((collection: collection) => {
                    if (collection.name == selectedCollection.name) {
                        var docsGrid = this.getDocumentsGrid();
                        if (!!docsGrid) {
                            docsGrid.refreshCollectionData();
                        }
                    } else {
                        var pagedList = collection.getDocuments();
                        pagedList.invalidateCache();
                    }
                });
            });
        }
    }

    selectCollection(collection: collection, event?: MouseEvent) {
        if (!event || event.which !== 3) {
            collection.activate();
            var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
            router.navigate(documentsWithCollectionUrl, false);
        }
    }

    selectColumns() {
        require(["viewmodels/selectColumns"], selectColumns => {

            // Fetch column widths from virtual table
            var virtualTable = this.getDocumentsGrid();
            var vtColumns = virtualTable.columns();
            this.currentColumnsParams().columns().forEach((column: customColumnParams) => {
                for (var i = 0; i < vtColumns.length; i++) {
                    if (column.binding() === vtColumns[i].binding) {
                        column.width(vtColumns[i].width() | 0);
                        break;
                    }
                }
            });

            var selectColumnsViewModel = new selectColumns(this.currentColumnsParams().clone(), this.currentCustomFunctions(), this.contextName(), this.activeDatabase());
            app.showDialog(selectColumnsViewModel);
            selectColumnsViewModel.onExit().done((cols) => {
                this.currentColumnsParams(cols);

                var pagedList = this.currentCollection().getDocuments();
                this.currentCollectionPagedItems(pagedList);
            });
        });
    }

    newDocument() {
        router.navigate(appUrl.forNewDoc(this.activeDatabase()));
    }

    toggleSelectAll() {
        var docsGrid = this.getDocumentsGrid();

        if (!!docsGrid) {
            if (this.hasAnyDocumentsSelected()) {
                docsGrid.selectNone();
            } else {
                docsGrid.selectSome();

                this.isAnyDocumentsAutoSelected(this.hasAllDocumentsSelected() == false);
            }
        }
    }

    selectAll() {
        var docsGrid = this.getDocumentsGrid();
        var c: collection = this.selectedCollection();

        if (!!docsGrid && !!c) {
            docsGrid.selectAll(c.documentCount());
        }
    }

    selectNone() {
        var docsGrid = this.getDocumentsGrid();

        if (!!docsGrid) {
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
        if (!this.selectedCollection().isSystemDocuments && this.hasAllDocumentsSelected()) {
            this.deleteCollection(this.selectedCollection());
        }
        else {
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
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

    private getDocumentsGrid(): virtualTable {
        var gridContents = $(documents.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = documents;