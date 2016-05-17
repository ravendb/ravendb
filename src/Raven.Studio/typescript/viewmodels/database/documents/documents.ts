import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import dynamicHeightBindingHandler = require("common/bindingHelpers/dynamicHeightBindingHandler");
import changesContext = require("common/changesContext");

import viewModelBase = require("viewmodels/viewModelBase");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");
import selectColumns = require("viewmodels/common/selectColumns");
import showDataDialog = require("viewmodels/common/showDataDialog");

import collection = require("models/database/documents/collection");
import database = require("models/resources/database");
import alert = require("models/database/debug/alert");
import document = require("models/database/documents/document");
import changeSubscription = require("common/changeSubscription");
import customFunctions = require("models/database/documents/customFunctions");
import customColumns = require("models/database/documents/customColumns");
import customColumnParams = require("models/database/documents/customColumnParams");
import collectionsStats = require("models/database/documents/collectionsStats");

import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import getCustomColumnsCommand = require("commands/database/documents/getCustomColumnsCommand");
import getEffectiveCustomFunctionsCommand = require("commands/database/globalConfig/getEffectiveCustomFunctionsCommand");
import getOperationAlertsCommand = require("commands/operations/getOperationAlertsCommand");
import dismissAlertCommand = require("commands/operations/dismissAlertCommand");
import getSingleAuthTokenCommand = require("commands/auth/getSingleAuthTokenCommand");
import generateClassCommand = require("commands/database/documents/generateClassCommand");

class documents extends viewModelBase {

    displayName = "documents";
    collections = ko.observableArray<collection>();
    documentsCount = ko.observable<number>(0);
    selectedCollection = ko.observable<collection>().subscribeTo("ActivateCollection").distinctUntilChanged();
    allDocumentsCollection: collection;
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
    currentExportUrl: KnockoutComputed<string>;
    isSystemDocumentsCollection: KnockoutComputed<boolean>;
    isRegularCollection: KnockoutComputed<boolean>;

    hasAnyDocumentsSelected: KnockoutComputed<boolean>;
    hasAllDocumentsSelected: KnockoutComputed<boolean>;
    isAnyDocumentsAutoSelected = ko.observable<boolean>(false);
    isAllDocumentsAutoSelected = ko.observable<boolean>(false);
    canCopyAllSelected: KnockoutComputed<boolean>;

    lastCollectionCountUpdate = ko.observable<string>();
    alerts = ko.observable<alert[]>([]);
    token = ko.observable<singleAuthToken>();
    static gridSelector = "#documentsGrid";
    static isInitialized = ko.observable<boolean>(false);
    isInitialized = documents.isInitialized;

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
        this.canCopyAllSelected = ko.computed(() => {
            this.showLoadingIndicator(); //triggers computing the new cached selected items
            var numOfSelectedDocuments = this.selectedDocumentIndices().length;
            var docsGrid = this.getDocumentsGrid();

            if (!!docsGrid) {
                const cachedItems = docsGrid.getNumberOfCachedItems();
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

        this.updateAuthToken();
        this.currentExportUrl = ko.computed(() => {
            var collection: collection = this.selectedCollection();
            if (this.isRegularCollection()) {
                return appUrl.forExportCollectionCsv(collection, collection.ownerDatabase) + (!!this.token() ? "&singleUseAuthToken=" + this.token().Token : "");
            }
            return null;
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
    }

    activate(args) {
        super.activate(args);
        if (args.withStop) {
            viewModelBase.hasContinueTestOption(true);
        }
        this.fetchCustomFunctions();
        this.updateHelpLink("G8CDCP");

        // We can optionally pass in a collection name to view's URL, e.g. #/documents?collection=Foo&database="blahDb"
        this.collectionToSelectName = args ? args.collection : null;

        var db = this.activeDatabase();
        this.fetchAlerts();
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
        //TODO: this.registerCollectionsResize();
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

        $(document).on("mousedown.collectionsResize", ".collection-resize", (e: JQueryMouseEventObject) => {
            startX = e.pageX;
            resizingColumn = true;
            startingLeftWidth = $("#documents-page-container").innerWidth();
            startingRightWidth = $("#documents-page-right-container").innerWidth();
            totalStartingWidth = startingLeftWidth + startingRightWidth;
        });

        $(document).on("mouseup.collectionsResize", "", () => {
            resizingColumn = false;
        });

        $(document).on("mousemove.collectionsResize", "", (e: JQueryMouseEventObject) => {
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
            }
            return false;
        });
    }

    unregisterCollectionsResizing() {
        $(document).off("mousedown.collectionsResize");
        $(document).off("mouseup.collectionsResize");
        $(document).off("mousemove.collectionsResize");
    }

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe("EditItem", () => this.editSelectedDoc()),
            ko.postbox.subscribe("ChangesApiReconnected", (db: database) => this.reloadDocumentsData(db)),
            ko.postbox.subscribe("SortCollections", () => this.sortCollections())
        ];
    }

    createNotifications(): Array<changeSubscription> {
        return [
            changesContext.currentResourceChangesApi().watchAllIndexes(() => this.refreshCollections()),
            changesContext.currentResourceChangesApi().watchAllDocs(() => this.refreshCollections()),
            changesContext.currentResourceChangesApi().watchBulks(() => this.refreshCollections())
        ];
    }

    private updateAuthToken() {
        new getSingleAuthTokenCommand(this.activeDatabase())
            .execute()
            .done(token => this.token(token));
    }

    exportCsv() {
        // schedule token update (to properly handle subseqent downloads)
        setTimeout(() => this.updateAuthToken(), 50);
        return true;
    }

    private fetchAlerts() {
        new getOperationAlertsCommand(this.activeDatabase())
            .execute()
            .then((result: alert[]) => {
                this.alerts(result);
            });
    }

    dismissAlert(uniqueKey: string) {
        new dismissAlertCommand(this.activeDatabase(), uniqueKey).execute();
        setTimeout(() => dynamicHeightBindingHandler.stickToTarget($(".ko-grid-viewport-container")[0], 'footer', 0), 25);
    }

    private fetchCollectionsStats(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db, this.collections()).execute();
    }

    private refreshCollections(): JQueryPromise<any> {
        var deferred = $.Deferred();
        var db = this.activeDatabase();

        this.fetchCollectionsStats(db).done(results => {
            this.documentsCount(results.numberOfDocuments());
            this.updateCollections(results.collections);
            this.refreshCollectionsData();
            //TODO: add a button to refresh the documents and than use this.refreshCollectionsData();
            deferred.resolve();
        });

        return deferred;
    }

    collectionsLoaded(collectionsStats: collectionsStats, db: database) {
        var collections = collectionsStats.collections;
        // Create the "All Documents" pseudo collection.
        this.allDocumentsCollection = collection.createAllDocsCollection(db);
        this.allDocumentsCollection.documentCount = collectionsStats.numberOfDocuments;

        // Create the "System Documents" pseudo collection.
        var systemDocumentsCollection = collection.createSystemDocsCollection(db);
        systemDocumentsCollection.documentCount = ko.computed(() => {
            var regularCollections = this.collections().filter((c: collection) => c.isAllDocuments === false && c.isSystemDocuments === false);
            if (regularCollections.length === 0)
                return 0;
            var sum = regularCollections.map((c: collection) => c.documentCount()).reduce((a, b) => a + b);
            return this.allDocumentsCollection.documentCount() - sum;
        });

        // All systems a-go. Load them into the UI and select the first one.
        var collectionsWithSysCollection = [systemDocumentsCollection].concat(collections);
        var allCollections = [this.allDocumentsCollection].concat(collectionsWithSysCollection);
        this.collections(allCollections);

        var collectionToSelect = allCollections.first(c => c.name === this.collectionToSelectName) || this.allDocumentsCollection;
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

    private deleteCollection(collection: collection) {
        if (collection) {
            var viewModel = new deleteCollection(collection);
            viewModel.deletionTask.done(() => {
                if (!collection.isAllDocuments) {
                    this.collections.remove(collection);

                    var selectedCollection: collection = this.selectedCollection();
                    if (collection.name === selectedCollection.name) {
                        this.selectCollection(this.allDocumentsCollection);
                    }
                } else {
                    this.selectNone();
                }

                this.updateGrid();
            });
            app.showDialog(viewModel);
        }
    }

    private updateGrid() {
        var selectedCollection: collection = this.selectedCollection();

        if (selectedCollection.isAllDocuments) {
            var docsGrid = this.getDocumentsGrid();
            docsGrid.refreshCollectionData();
        } else {
            var allDocumentsPagedList = this.allDocumentsCollection.getDocuments();
            allDocumentsPagedList.invalidateCache();
        }
    }

    private updateCollections(receivedCollections: Array<collection>) {
        var deletedCollections = [];

        this.collections().forEach((col: collection) => {
            if (!receivedCollections.first((receivedCol: collection) => col.name === receivedCol.name) && col.name !== "System Documents" && col.name !== "All Documents") {
                deletedCollections.push(col);
            }
        });

        this.collections.removeAll(deletedCollections);

        receivedCollections.forEach((receivedCol: collection) => {
            var foundCollection = this.collections().first((col: collection) => col.name === receivedCol.name);
            if (!foundCollection) {
                this.collections.push(receivedCol);
            } else {
                foundCollection.documentCount(receivedCol.documentCount());
            }
        });

        //if the collection is deleted, go to the all documents collection
        var currentCollection: collection = this.collections().first(c => c.name === this.selectedCollection().name);
        if (!currentCollection || currentCollection.documentCount() === 0) {
            this.selectCollection(this.allDocumentsCollection);
        }
    }

    private refreshCollectionsData() {
        var selectedCollection: collection = this.selectedCollection();

        this.collections().forEach((collection: collection) => {
            if (collection.name === selectedCollection.name) {
                var docsGrid = this.getDocumentsGrid();
                if (!!docsGrid) {
                    docsGrid.refreshCollectionData();
                }
            } else {
                var pagedList = collection.getDocuments();
                pagedList.invalidateCache();
            }
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
        if (!event || event.which !== 3) {
            collection.activate();
            var documentsWithCollectionUrl = appUrl.forDocuments(collection.name, this.activeDatabase());
            router.navigate(documentsWithCollectionUrl, false);
        }
    }

    selectColumns() {
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
        app.showDialog(selectColumnsViewModel);
        selectColumnsViewModel.onExit().done((cols) => {
            this.currentColumnsParams(cols);
            var pagedList = this.currentCollection().getDocuments();
            this.currentCollectionPagedItems(pagedList);
        });
    }

    newDocument() {
        router.navigate(appUrl.forNewDoc(this.activeDatabase()));
    }

    refresh() {
        this.getDocumentsGrid().refreshCollectionData();
        var selectedCollection = this.selectedCollection();
        selectedCollection.invalidateCache();
        this.selectNone();
    }

    toggleSelectAll() {
        var docsGrid = this.getDocumentsGrid();

        if (!!docsGrid) {
            if (this.hasAnyDocumentsSelected()) {
                docsGrid.selectNone();
            } else {
                docsGrid.selectSome();

                this.isAnyDocumentsAutoSelected(!this.hasAllDocumentsSelected());
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
        if (this.selectedCollection().isSystemDocuments === false && this.hasAllDocumentsSelected()) {
            this.deleteCollection(this.selectedCollection());
        } else {
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

    generateDocCode() {
        var grid = this.getDocumentsGrid();
        if (grid) {
            var selectedItem = <document>grid.getSelectedItems(1).first();

            var id = selectedItem.getId();
            new generateClassCommand(this.activeDatabase(), id, "csharp")
                .execute()
                .done((code: generatedCodeDto) => {
                    app.showDialog(new showDataDialog("Generated Class", code.Code));
                });
        }
    }

    private getDocumentsGrid(): virtualTable {
        var gridContents = $(documents.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }

    urlForAlert(alert: alert) {
        var index = this.alerts().indexOf(alert);
        return appUrl.forAlerts(this.activeDatabase()) + "&item=" + index;
    }

    private sortCollections() {
        this.collections.sort((c1: collection, c2: collection) => {
            if (c1.isAllDocuments)
                return -1;
            if (c2.isAllDocuments)
                return 1;
            if (c1.isSystemDocuments)
                return -1;
            if (c2.isSystemDocuments)
                return 1;
            return c1.name.toLowerCase() > c2.name.toLowerCase() ? 1 : -1;
        });
    }

    // Animation callbacks for the groups list
    showCollectionElement(element) {
        if (element.nodeType === 1 && documents.isInitialized()) {
            $(element).hide().slideDown(500, () => {
                ko.postbox.publish("SortCollections");
                $(element).highlight();
            });
        }
    }

    hideCollectionElement(element) {
        if (element.nodeType === 1) {
            $(element).slideUp(1000, () => { $(element).remove(); });
        }
    }
}

export = documents;
