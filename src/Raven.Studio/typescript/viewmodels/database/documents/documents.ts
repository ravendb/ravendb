import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
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

class documents extends viewModelBase {

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
    canExportAsCsv: KnockoutComputed<boolean>;
    
    $downloadForm: JQuery;

    private collectionToSelectName: string;
    private gridController = ko.observable<virtualGridController<document>>();
    private columnPreview = new columnPreviewPlugin<document>();
    columnsSelector = new columnsSelector<document>();

    spinners = {
        delete: ko.observable<boolean>(false),
        copy: ko.observable<boolean>(false)
    };

    constructor() {
        super();

        this.columnsSelector.configureColumnsPersistence(() => {
            if (this.currentCollection().isAllDocuments) {
                // don't save custom layout for all documents
                return null;
            }
            
            const dbName = this.activeDatabase().name;
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
            let selectedDocsCount = 0;
            const controll = this.gridController();
            if (controll) {
                selectedDocsCount = controll.selection().count;
            }
            return selectedDocsCount;
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
        
        this.canExportAsCsv = ko.pureComputed(() => {
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
        this.columnsSelector.reset();
        this.gridController().reset(true);
        this.setCurrentAsNotDirty();
    }

    setCurrentAsNotDirty() {
        this.dirtyCurrentCollection(false);
    }

    fetchDocs(skip: number, take: number, previewColumns: string[], fullColumns: string[]): JQueryPromise<pagedResultWithAvailableColumns<any>> {
        return this.currentCollection().fetchDocuments(skip, take, previewColumns, fullColumns);
    }

    compositionComplete() {
        super.compositionComplete();
        
        this.$downloadForm = $("#exportCsvForm");
        
        this.setupDisableReasons();

        const grid = this.gridController();

        grid.headerVisible(true);

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), grid, 
            { showRowSelectionCheckbox: true, enableInlinePreview: false, showSelectAllCheckbox: true, showFlags: true });

        this.columnsSelector.tryInitializeWithSavedDefault(source => documentsProvider.reviver(source));

        this.columnsSelector.init(grid, 
                                  (s, t, previewCols, fullCols) => this.fetchDocs(s, t, previewCols, fullCols),
                                  (w, r) => {
                                      if (this.currentCollection().isAllDocuments) {
                                          return [
                                              new checkedColumn(true),
                                              new hyperlinkColumn<document>(grid, document.createDocumentIdProvider(), x => appUrl.forEditDoc(x.getId(), this.activeDatabase()), "Id", "300px"),
                                              new textColumn<document>(grid, x => changeVectorUtils.formatChangeVectorAsShortString(x.__metadata.changeVector()), "Change Vector", "200px"),
                                              new textColumn<document>(grid, x => generalUtils.formatUtcDateAsLocal(x.__metadata.lastModified()), "Last Modified", "300px"),
                                              new hyperlinkColumn<document>(grid, x => x.getCollection(), x => appUrl.forDocuments(x.getCollection(), this.activeDatabase()), "Collection", "200px"),
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

        const fullDocumentsProvider = new documentPropertyProvider(this.activeDatabase());

        this.columnPreview.install(".documents-grid", ".js-documents-preview", 
            (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
            if (column instanceof textColumn) {
                if (this.currentCollection().isAllDocuments && column.header === "Last Modified") {
                    onValue(moment.utc(doc.__metadata.lastModified()), doc.__metadata.lastModified());
                } else if (this.currentCollection().isAllDocuments && column.header === "Change Vector") {
                    onValue(doc.__metadata.changeVector());
                } else {
                    fullDocumentsProvider.resolvePropertyValue(doc, column, (v: any) => {
                        if (!_.isUndefined(v)) {
                            const json = JSON.stringify(v, null, 4);
                            const html = Prism.highlight(json, (Prism.languages as any).javascript);
                            onValue(html, json);
                        }
                    }, error => {
                        const html = Prism.highlight("Unable to generate column preview: " + error.toString(), (Prism.languages as any).javascript);
                        onValue(html);
                    });
                }
            }
        });
    }

    private onCollectionSelected(newCollection: collection) {
        this.updateUrl(appUrl.forDocuments(newCollection.name, this.activeDatabase()));
        this.columnsSelector.reset();
        this.gridController().reset();
        this.setCurrentAsNotDirty();
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
        eventsCollector.default.reportEvent("document", "new-in-collection");
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
            const deleteDocsDialog = new deleteDocuments(selection.included.map(x => x.getId()), this.activeDatabase());

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
                        new deleteCollectionCommand(collectionName, this.activeDatabase(), excludedIds)
                            .execute()
                            .done((result: operationIdDto) => {
                                // Show progress details with the 'Delete by Collection' dialog
                                notificationCenter.instance.openDetailsForOperationById(this.activeDatabase(), result.OperationId); 

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
        this.gridController().reset(false);
        this.setCurrentAsNotDirty();
    }

    copySelectedDocs() {
        eventsCollector.default.reportEvent("documents", "copy");
        const selectedItems = this.gridController().getSelectedItems();

        this.spinners.copy(true);

        // get fresh copy of those documents, as grid might have incomplete information due to custom columns
        // or too long data
        new getDocumentsWithMetadataCommand(selectedItems.map(x => x.getId()), this.activeDatabase())
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

    exportCsvVisibleColumns() {
        const columns = this.columnsSelector.getSimpleColumnsFields();
        this.exportCsvInternal(columns);
    }
    
    exportCsvFull() {
        this.exportCsvInternal();
    }
    
    private exportCsvInternal(columns: string[] = undefined) {
        eventsCollector.default.reportEvent("documents", "export-csv");

        const args = {
            format: "csv",
            field: columns
        };

        const payload = {
            Query: "from '" + this.currentCollection().name + "'"
        };

        $("input[name=ExportOptions]").val(JSON.stringify(payload));

        const url = appUrl.forDatabaseQuery(this.activeDatabase()) + endpoints.databases.streaming.streamsQueries + appUrl.urlEncodeArgs(args);
        this.$downloadForm.attr("action", url);
        this.$downloadForm.submit();
    }
    
    queryCollection() {
        const query = queryCriteria.empty();
        const collection = this.currentCollection();
        const queryText = "from " + queryUtil.escapeCollectionOrFieldName(collection.collectionNameForQuery);
        
        query.queryText(queryText);
        query.name("Recent query (" + collection.collectionNameForQuery + ")");
        query.recentQuery(true);

        const queryDto = query.toStorageDto();
        const recentQueries = recentQueriesStorage.getSavedQueries(this.activeDatabase());
        recentQueriesStorage.appendQuery(queryDto, ko.observableArray(recentQueries));
        recentQueriesStorage.storeSavedQueries(this.activeDatabase(), recentQueries);

        const queryUrl = appUrl.forQuery(this.activeDatabase(), queryDto.hash);
        this.navigate(queryUrl);
    }
}

export = documents;
