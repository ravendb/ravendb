import app = require("durandal/app");
import router = require("plugins/router");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import deleteDocuments = require("viewmodels/common/deleteDocuments");
import deleteCollection = require("viewmodels/database/documents/deleteCollection");
import messagePublisher = require("common/messagePublisher");
import collectionsTracker = require("common/helpers/database/collectionsTracker");
import documentPropertyProvider = require("common/helpers/database/documentPropertyProvider");

import notificationCenter = require("common/notifications/notificationCenter");

import changesContext = require("common/changesContext");

import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import collectionsStats = require("models/database/documents/collectionsStats");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import getDocumentsWithMetadataCommand = require("commands/database/documents/getDocumentsWithMetadataCommand");

import eventsCollector = require("common/eventsCollector");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import checkedColumn = require("widgets/virtualGrid/columns/checkedColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import showDataDialog = require("viewmodels/common/showDataDialog");

class documents extends viewModelBase {

    static readonly copyLimit = 100;
    static readonly allDocumentCollectionName = "__all_docs";

    inSpecificCollection: KnockoutComputed<boolean>;
    deleteEnabled: KnockoutComputed<boolean>;
    private selectedItemsCount: KnockoutComputed<number>;

    dirtyResult = ko.observable<boolean>(false);
    dataChanged: KnockoutComputed<boolean>;
    tracker: collectionsTracker;

    copyDisabledReason: KnockoutComputed<disabledReason>;

    private collectionToSelectName: string;
    private gridController = ko.observable<virtualGridController<document>>();
    private columnPreview = new columnPreviewPlugin<document>();
    columnsSelector = new columnsSelector<document>();

    private fullDocumentsProvider: documentPropertyProvider;

    spinners = {
        delete: ko.observable<boolean>(false),
        copy: ko.observable<boolean>(false)
    }

    constructor() {
        super();

        this.initObservables();
    }

    private initObservables() {
        this.inSpecificCollection = ko.pureComputed(() => {
            const currentCollection = this.tracker.currentCollection();
            return currentCollection && !currentCollection.isAllDocuments;
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
                reason: `You can copy to up ${documents.copyLimit} documents.`
            }
        });
        this.dataChanged = ko.pureComputed(() => {
            const resultDirty = this.dirtyResult();
            const collectionChanged = this.tracker.dirtyCurrentCollection();
            return resultDirty || collectionChanged;
        });
    }

    activate(args: any) {
        super.activate(args);
        this.updateHelpLink("G8CDCP");

        this.collectionToSelectName = args ? args.collection : null;

        const db = this.activeDatabase();
        this.tracker = new collectionsTracker(db, () => this.gridController().resultEtag());
        this.fullDocumentsProvider = new documentPropertyProvider(this.activeDatabase());

        return this.fetchCollectionsStats(db).done(results => {
            this.collectionsLoaded(results, db);

            const dbStatsSubscription = changesContext.default.databaseNotifications()
                .watchAllDatabaseStatsChanged(event => this.tracker.onDatabaseStatsChanged(event));
            this.addNotification(dbStatsSubscription);
        });
    }

    private fetchCollectionsStats(db: database): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(db)
            .execute();
    }

    private collectionsLoaded(collectionsStats: collectionsStats, db: database) {
        let collections = collectionsStats.collections;
        collections = _.sortBy(collections, x => x.name.toLocaleLowerCase());

        //TODO: starred
        const allDocsCollection = collection.createAllDocumentsCollection(db, collectionsStats.numberOfDocuments());
        this.tracker.collections([allDocsCollection].concat(collections));

        const collectionToSelect = this.tracker.collections().find(x => x.name === this.collectionToSelectName) || allDocsCollection;
        this.tracker.currentCollection(collectionToSelect);
    }

    refresh() {
        eventsCollector.default.reportEvent("documents", "refresh");
        this.columnsSelector.reset();
        this.gridController().reset(true);
        this.tracker.setCurrentAsNotDirty();
    }

    fetchDocs(skip: number, take: number, previewColumns: string[], fullColumns: string[]): JQueryPromise<pagedResultWithAvailableColumns<any>> {
        return this.tracker.currentCollection().fetchDocuments(skip, take, previewColumns, fullColumns);
    }

    compositionComplete() {
        super.compositionComplete();

        this.setupDisableReasons();

        const documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.tracker.getCollectionNames(),
            { showRowSelectionCheckbox: true, enableInlinePreview: false, showSelectAllCheckbox: true });

        const grid = this.gridController();

        grid.headerVisible(true);

        this.columnsSelector.init(grid, (s, t, previewCols, fullCols) => this.fetchDocs(s, t, previewCols, fullCols), (w, r) => {
            if (this.tracker.currentCollection().isAllDocuments) {
                return [
                    new checkedColumn(true),
                    new hyperlinkColumn<document>(x => x.getId(), x => appUrl.forEditDoc(x.getId(), this.activeDatabase()), "Id", "300px"),
                    new textColumn<document>(x => x.__metadata.etag(), "ETag", "200px"),
                    new textColumn<document>(x => x.__metadata.lastModified(), "Last Modified", "300px"),
                    new hyperlinkColumn<document>(x => x.getCollection(), x => appUrl.forDocuments(x.getCollection(), this.activeDatabase()), "Collection", "200px")
                ];
            } else {
                return documentsProvider.findColumns(w, r);
            }
        }, (results: pagedResultWithAvailableColumns<document>) => results.availableColumns);

        grid.dirtyResults.subscribe(dirty => this.dirtyResult(dirty));

        this.tracker.currentCollection.subscribe(this.onCollectionSelected, this);

        this.columnPreview.install(".documents-grid", ".tooltip", (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
            if (column instanceof textColumn) {
                this.fullDocumentsProvider.resolvePropertyValue(doc, column, (v: any) => {
                    if (!_.isUndefined(v)) {
                        const json = JSON.stringify(v, null, 4);
                        const html = Prism.highlight(json, (Prism.languages as any).javascript);
                        onValue(html);    
                    }
                }, error => {
                    const html = Prism.highlight("Unable to generate column preview: " + error.toString(), (Prism.languages as any).javascript);
                    onValue(html);
                });
            }
        });
    }

    private onCollectionSelected(newCollection: collection) {
        this.updateUrl(appUrl.forDocuments(newCollection.name, this.activeDatabase()));
        this.columnsSelector.reset();
        this.gridController().reset();
        this.tracker.setCurrentAsNotDirty();
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
        const url = appUrl.forNewDoc(this.activeDatabase(), this.tracker.currentCollection().name);
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
            const deleteDocsDialog = new deleteDocuments(selection.included, this.activeDatabase());

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

            const deleteCollectionDialog = new deleteCollection(this.tracker.currentCollection().name, this.activeDatabase(), selection.count, excludedIds);
           
            app.showBootstrapDialog(deleteCollectionDialog)
                .done((deletionStarted: boolean) => {
                    if (deletionStarted) {
                        this.spinners.delete(true);

                        deleteCollectionDialog.operationIdTask.done((operationId: operationIdDto) => {
                            notificationCenter.instance.databseOperationsWatch.monitorOperation(operationId.OperationId)
                                .done(() => {
                                    if (excludedIds.length === 0) {
                                        messagePublisher.reportSuccess(`Deleted collection ${this.tracker.currentCollection().name}`);
                                    } else {
                                        messagePublisher.reportSuccess(`Deleted ${this.pluralize(selection.count, "document", "documents")} from ${this.tracker.currentCollection().name}`);
                                    }

                                    if (excludedIds.length === 0) {
                                        // deleted entire collection to go all documents
                                        const allDocsCollection = this.tracker.getAllDocumentsCollection();
                                        if (this.tracker.currentCollection() !== allDocsCollection) {
                                            this.tracker.currentCollection(allDocsCollection);
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
        this.tracker.setCurrentAsNotDirty();
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

    /* TODO:

    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe(EVENTS.ChangesApi.Reconnected, (db: database) => this.reloadDocumentsData(db)),
        ];
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

    currentColumnsParams = ko.observable<customColumns>(customColumns.empty());
    contextName = ko.observable<string>('');

    constructor() {
        super();
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

    selectCsvColumns() {
        eventsCollector.default.reportEvent("documents", "export-csv-custom-columns");
        var dialog = new selectCsvColumnsDialog(this.getDocumentsGrid().getColumnsNames());
        app.showBootstrapDialog(dialog);

        dialog.onExit().done((cols: string[]) => {
            this.exportCsvInternal(cols);
        });
    }

   

    */
}

export = documents;
