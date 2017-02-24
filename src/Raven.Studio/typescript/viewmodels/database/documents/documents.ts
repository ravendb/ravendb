import app = require("durandal/app");
import router = require("plugins/router");
import virtualTable = require("widgets/virtualTable/viewModel");
import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import EVENTS = require("common/constants/events");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");
import selectColumns = require("viewmodels/common/selectColumns");
import selectCsvColumnsDialog = require("viewmodels/common/selectCsvColumns");
import showDataDialog = require("viewmodels/common/showDataDialog");
import messagePublisher = require("common/messagePublisher");

import notificationCenter = require("common/notifications/notificationCenter");

import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
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

import virtualGrid = require("widgets/virtualGrid/virtualGrid");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import pagedResult = require("widgets/virtualGrid/pagedResult");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");

class documents extends viewModelBase {

    static readonly allDocumentCollectionName = "__all_docs";

    inSpecificCollection: KnockoutComputed<boolean>;

    private collectionToSelectName: string;
    private collections = ko.observableArray<collection>();
    private currentCollection = ko.observable<collection>();
    private gridController = ko.observable<virtualGridController<document>>();

    constructor() {
        super();

        this.initObservables();
    }

    private initObservables() {
        this.inSpecificCollection = ko.pureComputed(() => {
            const currentCollection = this.currentCollection();
            return currentCollection && !currentCollection.isAllDocuments;
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("G8CDCP");

        this.collectionToSelectName = args ? args.collection : null;

        const db = this.activeDatabase();
        return this.fetchCollectionsStats(db).done(results => {
            this.collectionsLoaded(results, db);
        });
    }

    private fetchCollectionsStats(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db)
            .execute();
    }

    private collectionsLoaded(collectionsStats: collectionsStats, db: database) {
        const collections = collectionsStats.collections;

        //TODO: starred
        const allDocsCollection = collection.createAllDocumentsCollection(db, collectionsStats.numberOfDocuments());
        this.collections([allDocsCollection].concat(collections));

        const collectionToSelect = this.collections().find(x => x.name === this.collectionToSelectName) || allDocsCollection;
        this.currentCollection(collectionToSelect);
    }

    private getCollectionNames() {
        return this.collections()
            .filter(x => !x.isAllDocuments && !x.isSystemDocuments)
            .map(x => x.name);
    }

    fetchDocs(skip: number, take: number): JQueryPromise<pagedResult<any>> {
        return this.currentCollection().fetchDocuments(skip, take);
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.getCollectionNames(), true, false);

        grid.headerVisible(true);
        grid.init((s, t) => this.fetchDocs(s, t), (w, r) => {
            if (this.currentCollection().isAllDocuments) {
                return [
                    new checkedColumn(),
                    new hyperlinkColumn<document>(x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.activeDatabase()), "Id", "300px"),
                    new textColumn<document>(x => x.__metadata.etag(), "ETag", "200px"),
                    new textColumn<document>(x => x.__metadata.lastModified(), "Last Modified", "300px"),
                    new hyperlinkColumn<document>(x => x.getCollection(), x => appUrl.forDocuments(x.getCollection(), this.activeDatabase()), "Collection", "200px")
                ];
            } else {
                return documentsProvider.findColumns(w, r);
            }
        });

        this.currentCollection.subscribe(this.onCollectionChanged, this);
    }

    private onCollectionChanged(newCollection: collection) {
        this.updateUrl(appUrl.forDocuments(newCollection.name, this.activeDatabase()));
        this.gridController().reset();
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

    deleteSelected() {
        const selection = this.gridController().selection();
        if (selection.count === 0) {
            throw new Error("No elements to delete");
        }

        if (selection.mode === "inclusive") {
            const idsToDelete = selection.included.map(x => x.getId());
            const deleteDocsDialog = new deleteDocuments(selection.included, this.activeDatabase());
            deleteDocsDialog.deletionTask.done(() => {
                this.gridController().reset();
            });

            app.showBootstrapDialog(deleteDocsDialog);
        } else {
            // exclusive
            const excludedIds = selection.excluded.map(x => x.getId());

            const deleteCollectionDialog = new deleteCollection(this.currentCollection().name, this.activeDatabase(), selection.count, excludedIds);

            deleteCollectionDialog.operationIdTask.done((operationId: operationIdDto) => {
                notificationCenter.instance.resourceOperationsWatch.monitorOperation(operationId.OperationId)
                    .done(() => {
                        if (excludedIds.length === 0) {
                            messagePublisher.reportSuccess(`Deleted collection ${this.currentCollection().name}`);
                        } else {
                            messagePublisher.reportSuccess(`Deleted ${this.pluralize(selection.count, "document", "documents")} from ${this.currentCollection().name}`);   
                        }

                        if (excludedIds.length === 0) {
                            // deleted entire collection to go all documents
                            const allDocsCollection = this.collections().find(x => x.isAllDocuments);
                            if (this.currentCollection() !== allDocsCollection) {
                                this.currentCollection(allDocsCollection);
                            }
                        }

                        this.gridController().reset();
                    });
            });
            app.showBootstrapDialog(deleteCollectionDialog);
        }
    }

    /*
    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    selectedDocumentsText: KnockoutComputed<string>;
    contextName = ko.observable<string>('');

    canCopyAllSelected: KnockoutComputed<boolean>;

    constructor() {
        super();

        this.selectedCollection.subscribe(c => this.selectedCollectionChanged(c));
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

    attached() {
        super.attached();
        super.createKeyboardShortcut("F2", () => this.editSelectedDoc(), "#documentsGrid");

        // Q. Why do we have to setup the grid shortcuts here, when the grid already catches these shortcuts?
        // A. Because if the focus isn't on the grid, but on the docs page itself, we still need to catch the shortcuts.
        var docsPageSelector = ".documents-page";
        this.createKeyboardShortcut("DELETE", () => this.getDocumentsGrid().deleteSelectedItems(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, D", () => this.copySelectedDocs(), docsPageSelector);
        this.createKeyboardShortcut("Ctrl+C, I",() => this.copySelectedDocIds(), docsPageSelector);
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
            });*
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

    

    refresh() {
        eventsCollector.default.reportEvent("documents", "refresh");
        this.getDocumentsGrid().refreshCollectionData();
        var selectedCollection = this.selectedCollection();
        selectedCollection.invalidateCache();
        this.selectNone();
        this.showCollectionChanged(false);
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
        if (element.nodeType === 1) {
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
