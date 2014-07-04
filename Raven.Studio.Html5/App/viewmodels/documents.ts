import app = require("durandal/app");
import router = require("plugins/router");
import shell = require("viewmodels/shell");

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
import changeSubscription = require('models/changeSubscription');
import changesApi = require("common/changesApi");
import customFunctions = require("models/customFunctions");
import getCustomFunctionsCommand = require("commands/getCustomFunctionsCommand");

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
    isSelectAll = ko.observable(false);
    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    contextName = ko.observable<string>('');
    currentCollection = ko.observable<collection>();
    modelPollingTimeoutFlag: boolean = true;
    isDocumentsUpToDate:boolean = true;
    showLoadingIndicator: KnockoutObservable<boolean> = ko.observable<boolean>(false);

    static gridSelector = "#documentsGrid";

    constructor() {
        super();
        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
        this.hasAnyDocumentsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);
    }

    activate(args) {
        super.activate(args);

        this.fetchCustomFunctions();

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

    createNotifications(): Array<changeSubscription> {
        return [
            shell.currentResourceChangesApi().watchAllDocs((e: documentChangeNotificationDto) => this.changesApiDocumentUpdated(e)),
            shell.currentResourceChangesApi().watchBulks((e: bulkInsertChangeNotificationDto) => this.changesApiBulkInsert(e))
        ];
    }

    private changesApiDocumentUpdated(e: documentChangeNotificationDto) {
        // treat document put/delete events
        this.isDocumentsUpToDate = false;
        var curCollection = this.collections.first(x => x.name === e.CollectionName);

        if (e.Type == documentChangeType.Delete) {
            if (!!curCollection) {
                if (curCollection.documentCount() == 1) {
                    this.collections.remove(curCollection);
                    this.allDocumentsCollection.clearCollection();
                    this.selectCollection(this.allDocumentsCollection);
                } else {
                    curCollection.documentCount(curCollection.documentCount() - 1);

                }
            }
        }

        this.throttledFetchCollections();
/*      TODO: Decide what to do when put event occur

        if (!curCollection) {
            var systemDocumentsCollection = this.collections.first(x => x.isSystemDocuments === true);
            if (!!systemDocumentsCollection && (!!e.CollectionName || (!!e.Id && e.Id.indexOf("Raven/Databases/") == 0))) {
                curCollection = systemDocumentsCollection;
            }
        }

        // for put event, if collection is recognized, increment collection and allDocuments count, if not, create new one also
        if (e.Type == documentChangeType.Put) {
            if (!!curCollection) {
                curCollection.documentCount(curCollection.documentCount() + 1);
            } else {
                curCollection = new collection(e.CollectionName, this.activeDatabase());
                curCollection.documentCount(1);
                this.collections.push(curCollection);
            }
            this.allDocumentsCollection.documentCount(this.allDocumentsCollection.documentCount() + 1);
            // for delete event, if collection is recognized, decrease collection and allDocuments count, if left with zero documents, delete collection
        } else if (e.Type == documentChangeType.Delete) {
            if (!!curCollection) {
                if (curCollection.documentCount() == 1) {
                    this.collections.remove(curCollection);
                } else {
                    curCollection.documentCount(curCollection.documentCount() - 1);
                }

                this.allDocumentsCollection.documentCount(this.allDocumentsCollection.documentCount() - 1);
            }
        }*/
    }

    private changesApiBulkInsert(e: bulkInsertChangeNotificationDto) {
        // treat bulk Insert events
        if (e.Type == documentChangeType.BulkInsertEnded) {
            this.isDocumentsUpToDate = false;

            this.throttledFetchCollections();
        }
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

    fetchCustomFunctions() {
        var customFunctionsCommand = new getCustomFunctionsCommand(this.activeDatabase()).execute();
        customFunctionsCommand.done((cf: customFunctions) => {
            this.currentCustomFunctions(cf);
        });
    }

    //TODO: this binding has notification leak!
    selectedCollectionChanged(selected: collection) {
        if (selected) {
            this.isSelectAll(false);

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

    updateCollections(receivedCollections: Array<collection>, db: database) {

        var deletedCollections = [];
        var curSelectedCollectionName = this.selectedCollection().name;
        

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
                receivedCol.fetchTotalDocumentCount();
            } else {
                foundCollection.fetchTotalDocumentCount();
            }
        });
        
        var collectionToSelect = this.collections().first(c => c.name === curSelectedCollectionName) || this.allDocumentsCollection;
        collectionToSelect.activate();
    }

    private throttledFetchCollections() {
        if (this.modelPollingTimeoutFlag === true) {
            
            setTimeout(() => {
                this.modelPollingTimeoutFlag = false;
                var db = appUrl.getDatabase();
                new getCollectionsCommand(db)
                    .execute()
                    .done(results => this.updateCollections(results, db)).always(() => {
                        this.isDocumentsUpToDate = true;
                        this.modelPollingTimeoutFlag = true;
                    });
                
            }, 5000);
        }
    }
    
    selectCollection(collection: collection) {
        collection.activate();
        
        var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
        router.navigate(documentsWithCollectionUrl, false);
    }

    selectColumns() {
        require(["viewmodels/selectColumns"], selectColumns => {
            var selectColumnsViewModel = new selectColumns(this.currentColumnsParams().clone(), this.currentCustomFunctions(), this.contextName(), this.activeDatabase());
            app.showDialog(selectColumnsViewModel);
            selectColumnsViewModel.onExit().done((cols) => {
                this.currentColumnsParams(cols);

                var pagedList = this.currentCollection().getDocuments();
                this.currentCollectionPagedItems(pagedList);
            });
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
