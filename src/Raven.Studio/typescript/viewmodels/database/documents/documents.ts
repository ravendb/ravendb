import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import generalUtils = require("common/generalUtils");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import changeVectorUtils = require("common/changeVectorUtils");
import documentPropertyProvider = require("common/helpers/database/documentPropertyProvider");
import notificationCenter = require("common/notifications/notificationCenter");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import getDocumentsWithMetadataCommand = require("commands/database/documents/getDocumentsWithMetadataCommand");
import deleteCollectionCommand = require("commands/database/documents/deleteCollectionCommand");
import eventsCollector = require("common/eventsCollector");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import flagsColumn = require("widgets/virtualGrid/columns/flagsColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import showDataDialog = require("viewmodels/common/showDataDialog");
import continueTest = require("common/shell/continueTest");
import queryCriteria = require("models/database/query/queryCriteria");
import recentQueriesStorage = require("common/storage/savedQueriesStorage");
import queryUtil = require("common/queryUtil");
import endpoints = require("endpoints");
import moment = require("moment");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database = require("models/resources/database");
import { highlight, languages } from "prismjs";
import getDocumentsPreviewCommand from "commands/database/documents/getDocumentsPreviewCommand";

class documents extends shardViewModelBase {
    
    view = require("views/database/documents/documents.html");

    static readonly copyLimit = 100;
    static readonly allDocumentCollectionName = "__all_docs";

    inSpecificCollection: KnockoutComputed<boolean>;
    deleteEnabled: KnockoutComputed<boolean>;
    selectedItemsCount: KnockoutComputed<number>;

    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;
    tracker = collectionsTracker.default;
    currentCollection = ko.observable<collection>();
    dirtyCurrentCollection = ko.observable<boolean>(false);

    copyDisabledReason: KnockoutComputed<disabledReason>;
    canExportToFile: KnockoutComputed<boolean>;
    
    $downloadForm: JQuery;

    itemsSoFar = ko.observable<number>(0);
    continuationToken: string;

    private collectionToSelectName: string;
    private gridController = ko.observable<virtualGridController<document>>();
    private columnPreview = new columnPreviewPlugin<document>();
    columnsSelector = new columnsSelector<document>();

    spinners = {
        delete: ko.observable<boolean>(false),
        copy: ko.observable<boolean>(false)
    };

    exportAsFileSettings = {
        format: ko.observable<"json" | "csv">("csv"),
        allColumns: ko.observable<boolean>(true)
    }

    constructor(db: database) {
        super(db);

        this.columnsSelector.configureColumnsPersistence(() => {
            if (this.currentCollection().isAllDocuments) {
                // don't save custom layout for all documents
                return null;
            }
            
            const dbName = this.db.name;
            const collectionName = this.currentCollection().name;
            
            return dbName + ".[" + collectionName + "]";
        });
        
        this.initObservables();
    }

    private initObservables() {
        this.inSpecificCollection = ko.pureComputed(() => {
            const currentCollection = this.currentCollection();
            return currentCollection && !currentCollection.isAllDocuments && !currentCollection.isRevisionsBin;
        });
        
        this.selectedItemsCount = ko.pureComputed(() => {
            const controller = this.gridController();
            if (controller) {
                return controller.selection().count;
            }
            return 0;
        });
        
        this.deleteEnabled = ko.pureComputed(() => {
            const deleteInProgress = this.spinners.delete();
            const selectedDocsCount = this.selectedItemsCount();

            return !deleteInProgress && selectedDocsCount > 0;
        });
        
        this.copyDisabledReason = ko.pureComputed<disabledReason>(() => {
            const count = this.selectedItemsCount();
            if (count === 0) {
                return { disabled: true };
            }
            if (count <= documents.copyLimit) {
                return { disabled: false };
            }

            return {
                disabled: true,
                reason: `You can only copy up to ${documents.copyLimit} documents`
            }
        });
        
        this.dataChanged = ko.pureComputed(() => {
            const resultDirty = this.dirtyResult();
            const collectionChanged = this.dirtyCurrentCollection();
            return resultDirty || collectionChanged;
        });
        
        this.canExportToFile = ko.pureComputed(() => {
            return this.inSpecificCollection();
        });
    }

    activate(args: any) {
        super.activate(args);

        continueTest.default.init(args);

        this.updateHelpLink("G8CDCP");

        this.collectionToSelectName = args ? args.collection : null;

        this.configureDirtyCollectionDetection();

        return collectionsTracker.default.loadStatsTask
            .done(() => {
                const collectionToSelect = this.tracker.collections().find(x => x.name === this.collectionToSelectName) || this.tracker.getAllDocumentsCollection();
                this.currentCollection(collectionToSelect);
            });
    }

    private configureDirtyCollectionDetection() {
        this.registerDisposable(this.tracker.registerOnCollectionCreatedHandler(c => {
            if (c.isAllDocuments) {
                this.dirtyCurrentCollection(true);
            }
        }));

        this.registerDisposable(this.tracker.registerOnCollectionRemovedHandler(c => {
            if (c === this.currentCollection()) {
                messagePublisher.reportWarning(c.name + " was removed");
                this.currentCollection(this.tracker.getAllDocumentsCollection());
            } else if (this.currentCollection().isAllDocuments) {
                this.dirtyCurrentCollection(true);
            }
        }));

        this.registerDisposable(this.tracker.registerOnCollectionUpdatedHandler((c, lastDocumentChangeVector) => {
            if (c.name === this.currentCollection().name) {
                if (lastDocumentChangeVector !== this.gridController().resultEtag()) {
                    this.dirtyCurrentCollection(true);
                }
            }
        }));

        this.registerDisposable(this.tracker.registerOnGlobalChangeVectorUpdatedHandler((changeVector: string) => {
            if (this.currentCollection().isAllDocuments) {
                if (changeVector && this.gridController().resultEtag() !== changeVector) {
                    this.dirtyCurrentCollection(true);
                }
            }
        }));
    }

    refresh() {
        eventsCollector.default.reportEvent("documents", "refresh");

        const documentsProvider = this.getDocumentsProvider();
        this.tryInitializeColumns(documentsProvider);
        
        this.resetGrid();
    }
    
    private resetGrid(hard = true) {
        this.itemsSoFar(0);
        this.continuationToken = undefined;
        this.gridController().reset(hard);
        this.setCurrentAsNotDirty();
    }

    setCurrentAsNotDirty() {
        this.dirtyCurrentCollection(false);
    }

    fetchDocs(skip: number, take: number, previewColumns: string[], fullColumns: string[]): JQueryPromise<pagedResultWithAvailableColumns<any>> {
        const collection = this.currentCollection().isAllDocuments ? undefined : this.currentCollection().name;
        return new getDocumentsPreviewCommand(this.db, skip, take, collection, previewColumns, fullColumns, this.continuationToken)
            .execute()
            .done((results => {
                this.continuationToken = results.continuationToken;
                // results.continuationToken will be null if there are no more results
                
                if (results.continuationToken) {
                    this.itemsSoFar(this.itemsSoFar() + results.items.length);
                    
                    if (this.itemsSoFar() === results.totalResultCount) {
                        results.totalResultCount = this.itemsSoFar();
                    } else {
                        results.totalResultCount = this.itemsSoFar() + 1;
                    }
                }
            }));
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.$downloadForm = $("#exportCsvForm");
        
        this.setupDisableReasons();

        const grid = this.gridController();

        grid.headerVisible(true);
        
        const documentsProvider = this.getDocumentsProvider();
        this.tryInitializeColumns(documentsProvider);

        this.columnsSelector.init(grid, 
                                  (s, t, previewCols, fullCols) => this.fetchDocs(s, t, previewCols, fullCols),
                                  (w, r) => {
                                      if (this.currentCollection().isAllDocuments) {
                                          return [
                                              new checkedColumn(true),
                                              new hyperlinkColumn<document>(grid, document.createDocumentIdProvider(), x => appUrl.forEditDoc(x.getId(), this.db), "Id", "300px"),
                                              new textColumn<document>(grid, x => changeVectorUtils.formatChangeVectorAsShortString(x.__metadata.changeVector()), "Change Vector", "200px"),
                                              new textColumn<document>(grid, x => generalUtils.formatUtcDateAsLocal(x.__metadata.lastModified()), "Last Modified", "300px"),
                                              new hyperlinkColumn<document>(grid, x => x.getCollection(), x => appUrl.forDocuments(x.getCollection(), this.db), "Collection", "200px"),
                                              new flagsColumn(grid)
                                          ];
                                      } else {
                                          return documentsProvider.findColumns(w, r);
                                      }
                                  }, 
                                  (results: pagedResultWithAvailableColumns<document>) => results.availableColumns
        );

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.currentCollection.subscribe(this.onCollectionSelected, this);

        const fullDocumentsProvider = new documentPropertyProvider(this.db);

        this.columnPreview.install(".documents-grid", ".js-documents-preview", 
            (doc: document, column: virtualColumn, e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
            if (column instanceof textColumn) {
                if (this.currentCollection().isAllDocuments && column.header === "Last Modified") {
                    onValue(moment.utc(doc.__metadata.lastModified()), doc.__metadata.lastModified());
                } else if (this.currentCollection().isAllDocuments && column.header === "Change Vector") {
                    onValue(doc.__metadata.changeVector());
                } else {
                    fullDocumentsProvider.resolvePropertyValue(doc, column, (v: any) => {
                        if (!_.isUndefined(v)) {
                            const json = JSON.stringify(v, null, 4);
                            const html = highlight(json, languages.javascript, "js");
                            onValue(html, json);
                        }
                    }, error => {
                        const html = highlight("Unable to generate column preview: " + error.toString(), languages.javascript, "js");
                        onValue(html);
                    });
                }
            }
        });
    }

    private getDocumentsProvider() {
        return new documentBasedColumnsProvider(this.db, this.gridController(),
            { showRowSelectionCheckbox: true, enableInlinePreview: false, showSelectAllCheckbox: true, showFlags: true });
    }
    
    private tryInitializeColumns(documentsProvider: documentBasedColumnsProvider) {
        this.columnsSelector.tryInitializeWithSavedDefault(source => documentsProvider.reviver(source));
    }

    private onCollectionSelected(newCollection: collection) {
        this.updateUrl(appUrl.forDocuments(newCollection.name, this.db));
        this.columnsSelector.reset();
        this.resetGrid(false);
    }

    newDocument(docs: documents, $event: JQuery.TriggeredEvent) {
        eventsCollector.default.reportEvent("document", "new");
        const url = appUrl.forNewDoc(this.db);
        if ($event.ctrlKey) {
            window.open(url);
        } else {
            router.navigate(url);
        }
    }

    newDocumentInCollection(docs: documents, $event: JQuery.TriggeredEvent) {
        eventsCollector.default.reportEvent("document", "new-in-collection");
        const url = appUrl.forNewDoc(this.db, this.currentCollection().name);
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
            const deleteDocsDialog = new deleteDocuments(selection.included.map(x => x.getId()), this.db);

            app.showBootstrapDialog(deleteDocsDialog)
                .done((deleting: boolean) => {
                    if (deleting) {
                        this.spinners.delete(true);

                        deleteDocsDialog.deletionTask
                            .always(() => this.onDeleteCompleted());
                    }
                });
        } else {
            // exclusive
            const excludedIds = selection.excluded.map(x => x.getId());

            const deleteCollectionDialog = new deleteCollection(this.currentCollection().name, selection.count);
            app.showBootstrapDialog(deleteCollectionDialog)
                .done((deleteWasRequested: boolean) => {
                    if (deleteWasRequested) {
                        this.spinners.delete(true);

                        const collectionName = this.currentCollection().name === collection.allDocumentsCollectionName ? "@all_docs" : this.currentCollection().name;
                        new deleteCollectionCommand(collectionName, this.db, excludedIds)
                            .execute()
                            .done((result: operationIdDto) => {
                                // Show progress details with the 'Delete by Collection' dialog
                                notificationCenter.instance.openDetailsForOperationById(this.db, result.OperationId); 

                                notificationCenter.instance.databaseOperationsWatch.monitorOperation(result.OperationId)
                                    .done(() => {
                                        if (excludedIds.length === 0) {
                                            messagePublisher.reportSuccess(`Deleted collection ${this.currentCollection().name}`);
                                        } else {
                                            messagePublisher.reportSuccess(`Deleted ${this.pluralize(selection.count, "document", "documents")} from ${this.currentCollection().name}`);
                                        }

                                        if (excludedIds.length === 0) {
                                            // if entire collection was deleted then go to 'all documents'
                                            const allDocsCollection = this.tracker.getAllDocumentsCollection();
                                            if (this.currentCollection() !== allDocsCollection) {
                                                this.currentCollection(allDocsCollection);
                                            }
                                        }
                                    })
                                    .always(() => this.onDeleteCompleted());
                            });
                    }
                });
        }
    }

    private onDeleteCompleted() {
        this.spinners.delete(false);
        this.resetGrid(false);
    }

    copySelectedDocs() {
        eventsCollector.default.reportEvent("documents", "copy");
        const selectedItems = this.gridController().getSelectedItems();

        this.spinners.copy(true);

        // get fresh copy of those documents, as grid might have incomplete information due to custom columns
        // or too long data
        new getDocumentsWithMetadataCommand(selectedItems.map(x => x.getId()), this.db)
            .execute()
            .done((results: Array<document>) => {
                const prettifySpacing = 4;
                const text = results.map(d => d.getId() + "\r\n" + JSON.stringify(d.toDto(false), null, prettifySpacing)).join("\r\n\r\n");

                app.showBootstrapDialog(new showDataDialog("Documents", text, "javascript"));
            })
            .always(() => this.spinners.copy(false));
    }

    copySelectedDocIds() {
        eventsCollector.default.reportEvent("documents", "copy-ids");

        const selectedItems = this.gridController().getSelectedItems();

        const text = selectedItems.map(x => '"' + x.getId() + '"').join(", \r\n");

        app.showBootstrapDialog(new showDataDialog("Document IDs", text, "javascript"));
    }

    exportAsFile() {
        eventsCollector.default.reportEvent("query", "export-csv");

        const args = {
            format: this.exportAsFileSettings.format(),
            field: this.exportAsFileSettings.allColumns() ? undefined : this.columnsSelector.getSimpleColumnsFields(),
        }
        
        const payload = {
            Query: "from '" + this.currentCollection().name + "'"
        };
        
        $("input[name=ExportOptions]").val(JSON.stringify(payload));

        const url = appUrl.forDatabaseQuery(this.db) + endpoints.databases.streaming.streamsQueries + appUrl.urlEncodeArgs(args);
        this.$downloadForm.attr("action", url);
        this.$downloadForm.submit();
    }
    
    queryCollection() {
        const query = queryCriteria.empty();
        const collection = this.currentCollection();
        const queryText = "from " + queryUtil.wrapWithSingleQuotes(collection.collectionNameForQuery);
        
        query.queryText(queryText);
        query.name("Recent query (" + collection.collectionNameForQuery + ")");
        query.recentQuery(true);

        const queryDto = query.toStorageDto();
        recentQueriesStorage.saveAndNavigate(this.db, queryDto);
    }
}

export = documents;
