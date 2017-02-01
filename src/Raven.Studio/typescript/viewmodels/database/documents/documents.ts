import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import EVENTS = require("common/constants/events");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");
import selectColumns = require("viewmodels/common/selectColumns");
import selectCsvColumnsDialog = require("viewmodels/common/selectCsvColumns");
import showDataDialog = require("viewmodels/common/showDataDialog");

import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import changeSubscription = require("common/changeSubscription");
import customFunctions = require("models/database/documents/customFunctions");
import customColumns = require("models/database/documents/customColumns");
import customColumnParams = require("models/database/documents/customColumnParams");
import collectionsStats = require("models/database/documents/collectionsStats");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");

import getCustomColumnsCommand = require("commands/database/documents/getCustomColumnsCommand");
import getEffectiveCustomFunctionsCommand = require("commands/database/globalConfig/getEffectiveCustomFunctionsCommand");
import generateClassCommand = require("commands/database/documents/generateClassCommand");

import eventsCollector = require("common/eventsCollector");

class documents extends viewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    collectionsExceptAllDocs: KnockoutComputed<collection[]>;
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection = ko.observable<collection>();
    collectionToSelectName: string;
    currentCollectionPagedItems = ko.observable<pagedList>();
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    currentCustomFunctions = ko.observable<customFunctions>(customFunctions.empty());
    selectedDocumentIndices = ko.observableArray<number>();
    selectedDocumentsText: KnockoutComputed<string>;
    hasDocuments: KnockoutComputed<boolean>;
    contextName = ko.observable<string>('');
    currentCollection = ko.observable<collection>();
    showLoadingIndicator = ko.observable<boolean>(false);
    showLoadingIndicatorThrottled = this.showLoadingIndicator.throttle(250);
    isSystemDocumentsCollection: KnockoutComputed<boolean>;
    isRegularCollection: KnockoutComputed<boolean>;

    documentsSelection: KnockoutComputed<checkbox>;
    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    hasAllDocumentsSelected: KnockoutComputed<boolean>;
    isAnyDocumentsAutoSelected = ko.observable<boolean>(false);
    isAllDocumentsAutoSelected = ko.observable<boolean>(false);
    canCopyAllSelected: KnockoutComputed<boolean>;

    lastCollectionCountUpdate = ko.observable<string>();
    static gridSelector = "#documentsGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = documents.isInitialized;
    showCollectionChanged = ko.observable<boolean>(false);

    documentsCount = ko.observable<number>(0);

    constructor() {
        super();

        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
        this.hasDocuments = ko.computed(() => {
            var selectedCollection: collection = this.selectedCollection();
            if (!!selectedCollection) {
                if (selectedCollection.name === collection.allDocsCollectionName) {
                    return this.documentsCount() > 0;
                }
                return this.selectedCollection().documentCount() > 0;
            }
            return false;
        });
        this.hasAnyDocumentsSelected = ko.computed(() => this.selectedDocumentIndices().length > 0);
        this.hasAllDocumentsSelected = ko.computed(() => {
            var numOfSelectedDocuments = this.selectedDocumentIndices().length;
            if (!!this.selectedCollection() && numOfSelectedDocuments !== 0) {
                return numOfSelectedDocuments === this.selectedCollection().documentCount();
            }
            return false;
        });
        this.documentsSelection = ko.computed(() => {
            var selected = this.selectedDocumentIndices();
            if (this.hasAllDocumentsSelected()) {
                return checkbox.Checked;
            }
            if (selected.length > 0) {
                return checkbox.SomeChecked;
            }
            return checkbox.UnChecked;
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

        this.isSystemDocumentsCollection = ko.computed(() => {
            var collection: collection = this.selectedCollection();
            return !!collection && collection.isSystemDocuments;
        });
        this.isRegularCollection = ko.computed(() => {
            var collection: collection = this.selectedCollection();
            return !!collection && !collection.isAllDocuments && !collection.isSystemDocuments;
        });

        this.selectedDocumentsText = ko.computed(() => {
            if (!!this.selectedDocumentIndices()) {
                var documentsText = "document";
                if (this.selectedDocumentIndices().length !== 1) {
                    documentsText += "s";
                }
                return documentsText;
            }
            return "";
        });
        
        this.collectionsExceptAllDocs = ko.computed(() => {
            var allDocs = this.allDocumentsCollection();
            if (!allDocs) {
                return [];
    }
            var collections = this.collections();
            return collections.filter(x => x !== allDocs);
        });
    }

    activate(args: any) {
        super.activate(args);
        if (args.withStop) {
            viewModelBase.hasContinueTestOption(true);
        }
        this.fetchCustomFunctions();
        this.updateHelpLink("G8CDCP");

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        var db = this.activeDatabase();
        this.fetchCollectionsStats(db).done(results => {
            this.collectionsLoaded(results, db);
            documents.isInitialized(true);
        });
    }

    attached() {
        super.attached();
        super.createKeyboardShortcut("F2", () => this.editSelectedDoc(), "#documentsGrid");

        // Q. Why do we have to setup the grid shortcuts here, when the grid already catches these shortcuts?
        // A. Because if the focus isn't on the grid, but on the docs page itself, we still need to catch the shortcuts.
        var docsPageSelector = ".documents-page";
        this.createKeyboardShortcut("DELETE", () => this.getDocumentsGrid().deleteSelectedItems(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, D", () => this.copySelectedDocs(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, I",() => this.copySelectedDocIds(), docsPageSelector);
        //this.registerCollectionsResize();
    }

    deactivate() {
        super.deactivate();
        documents.isInitialized(false);
        this.unregisterCollectionsResizing();
    }

    private registerCollectionsResize() {
        var resizingColumn = false;
        var startX = 0;
        var startingLeftWidth = 0;
        var startingRightWidth = 0;
        var totalStartingWidth = 0;

        $(document).on("mousedown.collectionsResize", ".collection-resize",(e: any) => {
            startX = e.pageX;
            resizingColumn = true;
            startingLeftWidth = $("#documents-page-container").innerWidth();
            startingRightWidth = $("#documents-page-right-container").innerWidth();
            totalStartingWidth = startingLeftWidth + startingRightWidth;
        });

        $(document).on("mouseup.collectionsResize", "", (e: any) => {
            resizingColumn = false;
        });

        $(document).on("mousemove.collectionsResize", "", (e: any) => {
            if (resizingColumn) {

                // compute new percentage values
                var diff = e.pageX - startX;
                
                $("#documents-page-container").innerWidth(((startingLeftWidth + diff) * 100.0 / totalStartingWidth) + "%");
                $("#documents-page-right-container").innerWidth(((startingRightWidth - diff) * 100.0 / totalStartingWidth) + "%");

                // Stop propagation of the event so the text selection doesn't fire up
                if (e.stopPropagation) e.stopPropagation();
                if (e.preventDefault) e.preventDefault();
                e.cancelBubble = true;
                e.returnValue = false;

                return false;
            }
        });
    }

    unregisterCollectionsResizing() {
        $(document).off("mousedown.collectionsResize");
        $(document).off("mouseup.collectionsResize");
        $(document).off("mousemove.collectionsResize");
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("ViewItem", () => this.editSelectedDoc()),
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => this.reloadDocumentsData(db)),
            ko.postbox.subscribe("SortCollections", () => this.sortCollections())
        ];
    }

    afterClientApiConnected(): void {
        const changesApi = this.changesContext.resourceChangesApi();
        this.addNotification(changesApi.watchAllDocs(() => this.refreshCollections()));

        //TODO: this.addNotification(changesApi.watchAllIndexes(() => this.refreshCollections()));
        //TODO: this.addNotification(changesApi.watchBulks(() => this.refreshCollections()));
    }

    exportCsv() {
        eventsCollector.default.reportEvent("documents", "export-csv");
        this.exportCsvInternal();
    }

    exportCsvInternal(customColumns?: string[]) {
        if (this.isRegularCollection()) {
            var collection: collection = this.selectedCollection();
            var db = this.activeDatabase();
            var url = appUrl.forExportCollectionCsv(collection, collection.ownerDatabase, customColumns);
            this.downloader.download(db, url);
        }
    }

    private fetchCollectionsStats(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db).execute();
    }

    private refreshCollections(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();

        this.fetchCollectionsStats(db)
            .done(results => {
                this.documentsCount(results.numberOfDocuments());
                this.updateCollections(results.collections);
                this.refreshCollectionsData();
                deferred.resolve();
            });

        return deferred;
    }

    collectionsLoaded(collectionsStats: collectionsStats, db: database) {
        var collections = collectionsStats.collections;
        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection(collection.createAllDocsCollection(db));
        this.allDocumentsCollection().documentCount(collectionsStats.numberOfDocuments());

        // All systems a-go. Load them into the UI and select the first one.
        var allCollections = [this.allDocumentsCollection()].concat(collections);
        this.collections(allCollections);

        var collectionToSelect = allCollections.find(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection();
        collectionToSelect.activate();
    }

    fetchCustomFunctions() {
        var customFunctionsCommand = new getEffectiveCustomFunctionsCommand(this.activeDatabase()).execute();
        customFunctionsCommand.done((cf: configurationDocumentDto<customFunctionsDto>) => {
            this.currentCustomFunctions(new customFunctions(cf.MergedDocument));
        });
    }

    //TODO: this binding has notification leak!
    selectedCollectionChanged(selected: collection) {
        if (!!selected) {
            var customColumnsCommand = selected.isAllDocuments ?
                getCustomColumnsCommand.forAllDocuments(this.activeDatabase()) : getCustomColumnsCommand.forCollection(selected.name, this.activeDatabase());

            this.contextName(customColumnsCommand.docName);

            customColumnsCommand.execute().done((dto: customColumnsDto) => {
                if (dto) {
                    this.currentColumnsParams().columns($.map(dto.Columns, c => new customColumnParams(c)));
                    this.currentColumnsParams().customMode(true);
                    selected.bindings(this.currentColumnsParams().getBindings());
                } else {
                    // use default values!
                    selected.bindings(undefined);
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
            var viewModel = new deleteCollection(collection, this.activeDatabase());
            viewModel.deletionTask.done((result: operationIdDto) => {
                if (!collection.isAllDocuments) {
                    this.collections.remove(collection);

                    var selectedCollection: collection = this.selectedCollection();
                    if (collection.name === selectedCollection.name) {
                        this.selectCollection(this.allDocumentsCollection());
                    }
                } else {
                    this.selectNone();
                }

                this.updateGridAfterOperationComplete(collection, result.OperationId);
            });
            app.showBootstrapDialog(viewModel);
        }
    }

    private updateGridAfterOperationComplete(collection: collection, operationId: number) {
        /* TODO var getOperationStatusTask = new getOperationStatusCommand(collection.ownerDatabase, operationId);
        getOperationStatusTask.execute()
             .done((result: bulkOperationStatusDto) => {
                if (result.Completed) {
                    var selectedCollection: collection = this.selectedCollection();

                    if (selectedCollection.isAllDocuments) {
                        var docsGrid = this.getDocumentsGrid();
                        docsGrid.refreshCollectionData();
                    } else {
                        var allDocumentsPagedList = this.allDocumentsCollection().getDocuments();
                        allDocumentsPagedList.invalidateCache();
                    }
                } else {
                    setTimeout(() => this.updateGridAfterOperationComplete(collection, operationId), 500);
                }
            });*/
    }

    private updateCollections(receivedCollections: Array<collection>) {
        var deletedCollections: collection[] = [];

        this.collections().forEach((col: collection) => {
            if (!receivedCollections.find((receivedCol: collection) => col.name === receivedCol.name) && col.name !== "System Documents" && col.name !== "All Documents") {
                deletedCollections.push(col);
            }
        });

        this.collections.removeAll(deletedCollections);

        //update collections, including collection count
        receivedCollections.forEach((receivedCol: collection) => {
            var foundCollection = this.collections().find((col: collection) => col.name === receivedCol.name);
            if (!foundCollection) {
                this.collections.push(receivedCol);
            } else {
                var oldCount = foundCollection.documentCount();
                var newCount = receivedCol.documentCount();
                var selectedCollection = this.selectedCollection();
                if (oldCount !== newCount) {
                    if (selectedCollection.name === receivedCol.name || selectedCollection.isAllDocuments) {
                        this.showCollectionChanged(true);
                    }
                }

                foundCollection.documentCount(receivedCol.documentCount());
            }
        });

        //if the collection is deleted, go to the all documents collection
        var currentCollection: collection = this.collections().find(c => c.name === this.selectedCollection().name);
        if (!currentCollection || currentCollection.documentCount() === 0) {
            this.selectCollection(this.allDocumentsCollection());
        }
    }

    private refreshCollectionsData() {
        this.collections().forEach((collection: collection) => {
            var pagedList = collection.getDocuments();
            pagedList.invalidateCache();
        });
    }

    private reloadDocumentsData(db: database) {
        if (db.name === this.activeDatabase().name) {
            this.refreshCollections().done(() => {
                this.refreshCollectionsData();
            });
        }
    }

    selectCollection(collection: collection, event?: MouseEvent) {
        const documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());

        if (event && event.ctrlKey) {
            window.open(documentsWithCollectionUrl);
            $(document.activeElement).blur();
        } else {
            router.navigate(documentsWithCollectionUrl, false);
            this.showCollectionChanged(false);
            collection.activate();
        }
    }

    selectCsvColumns() {
        eventsCollector.default.reportEvent("documents", "export-csv-custom-columns");
        var dialog = new selectCsvColumnsDialog(this.getDocumentsGrid().getColumnsNames());
        app.showBootstrapDialog(dialog);

        dialog.onExit().done((cols: string[]) => {
            this.exportCsvInternal(cols);
        });
    }

    selectColumns() {
        eventsCollector.default.reportEvent("documents", "select-columns");
        // Fetch column widths from virtual table
        var virtualTable = this.getDocumentsGrid();
        var columnsNames = virtualTable.getColumnsNames();
        var vtColumns = virtualTable.columns();
        this.currentColumnsParams().columns().forEach((column: customColumnParams) => {
            for (var i = 0; i < vtColumns.length; i++) {
                if (column.binding() === vtColumns[i].binding) {
                    column.width(vtColumns[i].width() | 0);
                    break;
                }
            }
        });

        var selectColumnsViewModel = new selectColumns(this.currentColumnsParams().clone(), this.currentCustomFunctions(), this.contextName(), this.activeDatabase(), columnsNames);
        app.showBootstrapDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols) => {
            this.currentColumnsParams(cols);
            this.currentCollection().bindings(this.currentColumnsParams().getBindings());
            var pagedList = this.currentCollection().getDocuments();
            pagedList.invalidateCache();
            this.currentCollectionPagedItems(pagedList);
        });
    }

    newDocument(docs: documents, $event: JQueryEventObject) {
        eventsCollector.default.reportEvent("document", "new");
        const url = appUrl.forNewDoc(this.activeDatabase());
        if ($event.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    }

    newDocumentInCollection(docs: documents, $event: JQueryEventObject) {
        const url = appUrl.forNewDoc(this.activeDatabase(), this.currentCollection().name);
        if ($event.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    }

    refresh() {
        eventsCollector.default.reportEvent("documents", "refresh");
        this.getDocumentsGrid().refreshCollectionData();
        var selectedCollection = this.selectedCollection();
        selectedCollection.invalidateCache();
        this.selectNone();
        this.showCollectionChanged(false);
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
        eventsCollector.default.reportEvent("document", "edit");
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.editLastSelectedItem();
        }
    }

    deleteSelectedDocs() {
        if (this.selectedCollection().isSystemDocuments === false && this.hasAllDocumentsSelected() && !this.selectedCollection().isAllDocuments) {
            eventsCollector.default.reportEvent("collection", "delete");
            this.deleteCollection(this.selectedCollection());
        } else {
            eventsCollector.default.reportEvent("documents", "delete");
            var grid = this.getDocumentsGrid();
            if (grid) {
                grid.deleteSelectedItems();
            }
        }
    }

    copySelectedDocs() {
        eventsCollector.default.reportEvent("documents", "copy");
        var grid = this.getDocumentsGrid();
        if (grid) {
            grid.copySelectedDocs();
        }
    }

    copySelectedDocIds() {
        eventsCollector.default.reportEvent("documents", "copy-ids");
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

    private sortCollections() {
        this.collections.sort((c1: collection, c2: collection) => {
            if (c1.isAllDocuments)
                return -1;
            if (c2.isAllDocuments)
                return 1;
            return c1.name.toLowerCase() > c2.name.toLowerCase() ? 1 : -1;
        });
    }

    // Animation callbacks for the groups list
    showCollectionElement(element: Element) {
        if (element.nodeType === 1 && documents.isInitialized()) {
            $(element).hide().slideDown(500, () => {
                ko.postbox.publish("SortCollections");
                $(element).highlight();
            });
        }
    }

    hideCollectionElement(element: Element) {
        if (element.nodeType === 1) {
            $(element).slideUp(1000, () => { $(element).remove(); });
        }
    }


    /* TODO 
    generateDocCode() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            var selectedItem = <Document>grid.getSelectedItems(1)[0];

            var metadata = (<any>selectedItem)["__metadata"];
            var id = metadata["id"];
            var generate = new generateClassCommand(this.activeDatabase(), id, "csharp");
            var deffered = generate.execute();
            deffered.done((code: any) => {
                app.showBootstrapDialog(new showDataDialog("Generated Class", code["Code"]));
            });
        }
    }
    */
}

export = documents;
