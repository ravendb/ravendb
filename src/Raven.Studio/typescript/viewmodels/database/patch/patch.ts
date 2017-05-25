import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import messagePublisher = require("common/messagePublisher");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import savePatchCommand = require('commands/database/patch/savePatchCommand');
import patchByQueryCommand = require("commands/database/patch/patchByQueryCommand");
import patchByCollectionCommand = require("commands/database/patch/patchByCollectionCommand");
import queryUtil = require("common/queryUtil");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');
import eventsCollector = require("common/eventsCollector");
import notificationCenter = require("common/notifications/notificationCenter");
import queryCriteria = require("models/database/query/queryCriteria");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");
import popoverUtils = require("common/popoverUtils");
import documentMetadata = require("models/database/documents/documentMetadata");
import deleteDocumentsCommand = require("commands/database/documents/deleteDocumentsCommand");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import columnsSelector = require("viewmodels/partial/columnsSelector");
import documentPropertyProvider = require("common/helpers/database/documentPropertyProvider");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import patchDocumentCommand = require("commands/database/documents/patchDocumentCommand");
import showDataDialog = require("viewmodels/common/showDataDialog");
import verifyDocumentsIDsCommand = require("commands/database/documents/verifyDocumentsIDsCommand");
import generalUtils = require("common/generalUtils");

type fetcherType = (skip: number, take: number, previewCols: string[], fullCols: string[]) => JQueryPromise<pagedResult<document>>;

class patchList {

    previewItem = ko.observable<patchDocument>();

    private allPatches = ko.observableArray<patchDocument>([]);

    private readonly useHandler: (patch: patchDocument) => void;
    private readonly removeHandler: (patch: patchDocument) => void;

    hasAnySavedPatch = ko.pureComputed(() => this.allPatches().length > 0);

    previewCode = ko.pureComputed(() => {
        const item = this.previewItem();
        if (!item) {
            return "";
        }

        return Prism.highlight(item.script(), (Prism.languages as any).javascript);
    });

    constructor(useHandler: (patch: patchDocument) => void, removeHandler: (patch: patchDocument) => void) {
        _.bindAll(this, ...["previewPatch", "removePatch", "usePatch"] as Array<keyof this>);
        this.useHandler = useHandler;
        this.removeHandler = removeHandler;
    }

    filteredPatches = ko.pureComputed(() => {
        let text = this.filters.searchText();

        if (!text) {
            return this.allPatches();
        }

        text = text.toLowerCase();

        return this.allPatches().filter(x => x.name().toLowerCase().includes(text));
    });

    filters = {
        searchText: ko.observable<string>()
    }

    previewPatch(item: patchDocument) {
        this.previewItem(item);
    }

    usePatch() {
        this.useHandler(this.previewItem());
    }

    removePatch(item: patchDocument) {
        if (this.previewItem() === item) {
            this.previewItem(null);
        }
        this.removeHandler(item);
    }

    loadAll(db: database) {
        return new getPatchesCommand(db)
            .execute()
            .done((patches: patchDocument[]) => {
                this.allPatches(patches);

                if (this.filteredPatches().length) {
                    this.previewItem(this.filteredPatches()[0]);
                }
            });
    }
}

class patchTester extends viewModelBase {

    testMode = ko.observable<boolean>(false);
    script: KnockoutObservable<string>;
    private scriptCopy: string;
    documentId = ko.observable<string>();
    private db: KnockoutObservable<database>;

    beforeDoc = ko.observable<any>();
    afterDoc = ko.observable<any>();

    actions = {
        loadDocument: ko.observableArray<string>(),
        putDocument: ko.observableArray<any>(),
        info: ko.observableArray<string>()
    }

    showObjectsInPutSection = ko.observable<boolean>(false);

    spinners = {
        testing: ko.observable<boolean>(false),
        loadingDocument: ko.observable<boolean>(false)
    }

    documentIdSearchResults = ko.observableArray<string>([]);

    validationGroup: KnockoutValidationGroup;

    constructor(script: KnockoutObservable<string>, db: KnockoutObservable<database>) {
        super();
        this.script = script;
        this.db = db;
        this.initObservables();

        this.bindToCurrentInstance("closeTestMode", "enterTestMode", "applyTestScript", "runTest", "onAutocompleteOptionSelected");

        this.validationGroup = ko.validatedObservable({
            script: this.script,
            documentId: this.documentId
        });
    }

    formatAsJson(input: KnockoutObservable<any> | any) {
        return ko.pureComputed(() => {
            const value = ko.unwrap(input);
            if (_.isUndefined(value)) {
                return "";
            } else {
                const json = JSON.stringify(value, null, 4);
                return Prism.highlight(json, (Prism.languages as any).javascript);
            }
        });
    }

    private initObservables() {
        this.testMode.subscribe(testMode => {
            patch.$body.toggleClass('show-test', testMode);
        });

        this.documentId.extend({
            required: true
        });

        this.documentId.throttle(250).subscribe(item => {
            patch.fetchDocumentIdAutocomplete(item, this.db(), this.documentIdSearchResults);
        });

        patch.setupDocumentIdValidation(this.documentId, this.db, () => true);
    }

    closeTestMode() {
        this.script(this.scriptCopy);
        this.testMode(false);
    }

    enterTestMode(documentIdToUse: string) {
        this.scriptCopy = this.script();
        this.testMode(true);
        this.documentId(documentIdToUse);

        this.validationGroup.errors.showAllMessages(false);

        if (documentIdToUse) {
            this.loadDocument();
        }
    }

    resetForm() {
        this.actions.loadDocument([]);
        this.actions.putDocument([]);
        this.actions.info([]);
        this.afterDoc(undefined);
        this.beforeDoc(undefined);
    }

    loadDocument() {
        this.resetForm();

        this.spinners.loadingDocument(true);

        new getDocumentWithMetadataCommand(this.documentId(), this.db())
            .execute()
            .done((doc: document) => {
                if (doc) {
                    this.beforeDoc(doc.toDto(true));
                }
            })
            .fail((xhr: JQueryXHR) => {
                if (xhr.status === 404) {
                    messagePublisher.reportWarning("Document doesn't exist.");
                } else {
                    messagePublisher.reportError("Failed to load document.", xhr.responseText, xhr.statusText);
                }
            })
            .always(() => this.spinners.loadingDocument(false));
    }

    applyTestScript() {
        this.testMode(false);
    }

    onAutocompleteOptionSelected(item: string) {
        this.documentId(item);
        this.loadDocument();
    }

    runTest() {
        eventsCollector.default.reportEvent("patch", "test");

        this.afterAsyncValidationCompleted(this.validationGroup, () => {
            if (this.isValid(this.validationGroup)) {
                this.spinners.testing(true);
                this.resetForm();

                new patchDocumentCommand(this.documentId(), this.script(), true, this.db())
                    .execute()
                    .done((result) => {
                        this.beforeDoc(result.OriginalDocument);
                        this.afterDoc(result.ModifiedDocument);
                        const debug = result.Debug;
                        const actions = debug.Actions as Raven.Server.Documents.Patch.PatchDebugActions;
                        this.actions.loadDocument(actions.LoadDocument);
                        this.actions.putDocument(actions.PutDocument);
                        this.actions.info(debug.Info);

                        if (result.Status === "Patched") {
                            messagePublisher.reportSuccess("Test completed");
                        }
                    })
                    .fail((xhr: JQueryXHR) => {
                        if (xhr.status === 404) {
                            messagePublisher.reportWarning("Test failed: Document doesn't exist.");
                        } else {
                            messagePublisher.reportError("Failed to test patch.", xhr.responseText, xhr.statusText);
                        }
                    })
                    .always(() => this.spinners.testing(false));
            }
        });
    }
}


class patch extends viewModelBase {

    static readonly $body = $("body");

    inSaveMode = ko.observable<boolean>();
    patchSaveName = ko.observable<string>();

    spinners = {
        save: ko.observable<boolean>(false),
        preview: ko.observable<boolean>(false)
    }

    gridController = ko.observable<virtualGridController<document>>();
    private documentsProvider: documentBasedColumnsProvider;
    private columnPreview = new columnPreviewPlugin<document>();
    columnsSelector = new columnsSelector<document>();
    private fullDocumentsProvider: documentPropertyProvider;
    private fetcher = ko.observable<fetcherType>();

    patchDocument = ko.observable<patchDocument>(patchDocument.empty());

    indexNames = ko.observableArray<string>();
    indexFields = ko.observableArray<string>();
    collections = ko.observableArray<collection>([]);

    isDocumentMode: KnockoutComputed<boolean>;
    isCollectionMode: KnockoutComputed<boolean>;
    isIndexMode: KnockoutComputed<boolean>;

    documentIdSearchResults = ko.observableArray<string>();

    runPatchValidationGroup: KnockoutValidationGroup;
    runQueryValidationGroup: KnockoutValidationGroup;
    savePatchValidationGroup: KnockoutValidationGroup;
    previewDocumentValidationGroup: KnockoutValidationGroup;

    savedPatches = new patchList(item => this.usePatch(item), item => this.removePatch(item));

    test = new patchTester(this.patchDocument().script, this.activeDatabase);

    private hideSavePatchHandler = (e: Event) => {
        if ($(e.target).closest(".patch-save").length === 0) {
            this.inSaveMode(false);
        }
    }

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.initValidation();

        this.bindToCurrentInstance("usePatchOption", "useIndex", "useCollection", "previewDocument");
        this.initObservables();
    }

    static setupDocumentIdValidation(field: KnockoutObservable<string>, db: KnockoutObservable<database>, onlyIf: () => boolean) {
        const verifyDocuments = (val: string, params: any, callback: (currentValue: string, result: boolean) => void) => {
            new verifyDocumentsIDsCommand([val], db())
                .execute()
                .done((ids: string[]) => {
                    callback(field(), ids.length > 0);
                });
        };

        field.extend({
            required: true,
            validation: {
                message: "Document doesn't exist.",
                async: true,
                onlyIf: onlyIf,
                validator: generalUtils.debounceAndFunnel(verifyDocuments)
            }
        });
    }

    private initValidation() {
        const doc = this.patchDocument();

        doc.script.extend({
            required: true
        });

        doc.selectedItem.extend({
            required: true
        });

        patch.setupDocumentIdValidation(doc.selectedItem,
            this.activeDatabase,
            () => this.patchDocument().patchOnOption() === "Document");

        this.patchSaveName.extend({
            required: true
        });

        this.runPatchValidationGroup = ko.validatedObservable({
            script: doc.script,
            selectedItem: doc.selectedItem
        });
        this.runQueryValidationGroup = ko.validatedObservable({
            selectedItem: doc.selectedItem
        });

        this.savePatchValidationGroup = ko.validatedObservable({
            patchSaveName: this.patchSaveName
        });

        this.previewDocumentValidationGroup = ko.validatedObservable({
            selectedItem: doc.selectedItem
        });
    }

    private initObservables() {
        this.isDocumentMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Document");
        this.isCollectionMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Collection");
        this.isIndexMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Index");

        this.patchDocument().selectedItem.throttle(250).subscribe(item => {
            if (this.patchDocument().patchOnOption() === "Document") {
                patch.fetchDocumentIdAutocomplete(item, this.activeDatabase(), this.documentIdSearchResults);
            }
        });

        this.patchDocument().patchAll.subscribe((patchAll) => {
            this.documentsProvider.showRowSelectionCheckbox = !patchAll;

            this.columnsSelector.reset();
            this.gridController().reset(true);
        });

        this.inSaveMode.subscribe(enabled => {
            const $input = $(".patch-save .form-control");
            if (enabled) {
                $input.show();
                window.addEventListener("click", this.hideSavePatchHandler, true);
            } else {
                this.savePatchValidationGroup.errors.showAllMessages(false);
                window.removeEventListener("click", this.hideSavePatchHandler, true);
                setTimeout(() => $input.hide(), 200);
            }
        });
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");

        this.fullDocumentsProvider = new documentPropertyProvider(this.activeDatabase());

        return $.when<any>(this.fetchAllCollections(), this.fetchAllIndexes(), this.savedPatches.loadAll(this.activeDatabase()));
    }

    attached() {
        super.attached();

        const jsCode = Prism.highlight("this.NewProperty = this.OldProperty + myParameter;\r\n" +
            "delete this.UnwantedProperty;\r\n" +
            "this.Comments.RemoveWhere(function(comment){\r\n" +
            "  return comment.Spam;\r\n" +
            "});",
            (Prism.languages as any).javascript);

        $(".query-label small").popover({
            html: true,
            trigger: "hover",
            template: popoverUtils.longPopoverTemplate,
            container: 'body',
            content: '<p>Queries use Lucene syntax. Examples:</p><pre><span class="token keyword">Name</span>: Hi?berna*<br/><span class="token keyword">Count</span>: [0 TO 10]<br/><span class="token keyword">Title</span>: "RavenDb Queries 1010" <span class="token keyword">AND Price</span>: [10.99 TO *]</pre>'
        });

        $(".patch-title small").popover({
            html: true,
            trigger: "hover",
            container: "body",
            template: popoverUtils.longPopoverTemplate,
            content: `<p>Patch Scripts are written in JavaScript. <br />Examples: <pre>${jsCode}</pre></p>`
        });
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        this.documentsProvider = new documentBasedColumnsProvider(this.activeDatabase(), this.collections().map(x => x.name), {
            showRowSelectionCheckbox: false,
            showSelectAllCheckbox: false
        });

        const fakeFetcher: fetcherType = () => $.Deferred<pagedResult<document>>().resolve({
            items: [],
            totalResultCount: -1
        });

        grid.headerVisible(true);

        const allColumnsProvider = (results: pagedResultWithAvailableColumns<document>) => {
            const selectedItem = this.patchDocument().selectedItem();
            if (!selectedItem || this.patchDocument().patchOnOption() === "Document" || !this.fetcher()) {
                return [];
            }

            switch (this.patchDocument().patchOnOption()) {
                case "Document":
                    return [];
                case "Index":
                    return documentBasedColumnsProvider.extractUniquePropertyNames(results);
                case "Collection":
                    return results.availableColumns;
            }
        };

        this.columnsSelector.init(grid, (s, t, previewCols, fullCols) => this.fetcher() ? this.fetcher()(s, t, previewCols, fullCols) : fakeFetcher(s, t, [], []),
            (w, r) => this.documentsProvider.findColumns(w, r),
            allColumnsProvider);

        this.columnPreview.install(".patch-grid", ".tooltip", (doc: document, column: virtualColumn, e: JQueryEventObject, onValue: (context: any) => void) => {
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

        this.fetcher.subscribe(() => grid.reset());
    }

    usePatchOption(option: patchOption) {
        this.fetcher(null);

        const patchDoc = this.patchDocument();
        patchDoc.selectedItem(null);
        patchDoc.patchOnOption(option);
        patchDoc.patchAll(option === "Index" || option === "Collection");

        if (option !== "Index") {
            patchDoc.query(null);
        }

        this.runPatchValidationGroup.errors.showAllMessages(false);
    }

    useIndex(indexName: string) {
        const patchDoc = this.patchDocument();
        patchDoc.selectedItem(indexName);
        patchDoc.patchAll(true);

        this.columnsSelector.reset();

        queryUtil.fetchIndexFields(this.activeDatabase(), indexName, this.indexFields);

        this.runQuery();
    }

    useCollection(collectionToUse: collection) {
        this.columnsSelector.reset();

        const fetcher = (skip: number, take: number, previewCols: string[], fullCols: string[]) => collectionToUse.fetchDocuments(skip, take, previewCols, fullCols);
        this.fetcher(fetcher);

        const patchDoc = this.patchDocument();
        patchDoc.selectedItem(collectionToUse.name);
        patchDoc.patchAll(true);
    }

    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        queryUtil.queryCompleter(this.indexFields, this.patchDocument().selectedIndex, this.activeDatabase, editor, session, pos, prefix, callback);
    }

    usePatch(item: patchDocument) {
        const patchDoc = this.patchDocument();

        //TODO: handle case when saved patch has collection which no longer exist, or index which is not available

        patchDoc.copyFrom(item);

        switch (patchDoc.patchOnOption()) {
            case "Index":
                this.useIndex(patchDoc.selectedItem());
                break;
            case "Collection":
                const matchedCollection = this.collections().find(x => x.name === patchDoc.selectedItem());
                if (matchedCollection) {
                    this.useCollection(matchedCollection);
                }
                break;
        }
    }

    removePatch(item: patchDocument) {
        this.confirmationMessage("Patch", `Are you sure you want to delete patch '${item.name()}'?`, ["Cancel", "Delete"])
            .done(result => {
                if (result.can) {
                    new deleteDocumentsCommand([item.getId()], this.activeDatabase())
                        .execute()
                        .done(() => {
                            messagePublisher.reportSuccess("Deleted patch " + item.name());
                            this.savedPatches.loadAll(this.activeDatabase());
                        })
                        .fail(response => messagePublisher.reportError("Failed to delete " + item.name(), response.responseText, response.statusText));
                }
            });
    }

    runQuery(): void {
        if (this.isValid(this.runQueryValidationGroup)) {
            const selectedIndex = this.patchDocument().selectedItem();
            if (selectedIndex) {
                const database = this.activeDatabase();
                const query = this.patchDocument().query();

                const resultsFetcher = (skip: number, take: number) => {
                    const criteria = queryCriteria.empty();
                    criteria.selectedIndex(selectedIndex);
                    criteria.queryText(query);

                    return new queryIndexCommand(database, skip, take, criteria)
                        .execute();
                };
                this.fetcher(resultsFetcher);
            }
        }
    }

    runPatch() {
        if (this.isValid(this.runPatchValidationGroup)) {

            const patchDoc = this.patchDocument();

            switch (patchDoc.patchOnOption()) {
            case "Document":
                this.patchOnDocuments([patchDoc.selectedItem()]);
                break;
            case "Index":
                if (patchDoc.patchAll()) {
                    this.patchOnIndex();
                } else {
                    const selectedIds = this.gridController().getSelectedItems().map(x => x.getId());
                    this.patchOnDocuments(selectedIds);
                }
                break;
            case "Collection":
                if (patchDoc.patchAll()) {
                    this.patchOnCollection();
                } else {
                    const selectedIds = this.gridController().getSelectedItems().map(x => x.getId());
                    this.patchOnDocuments(selectedIds);
                }
            }
        }
    }

    savePatch() {
        if (this.inSaveMode()) {
            eventsCollector.default.reportEvent("patch", "save");

            if (this.isValid(this.savePatchValidationGroup)) {
                this.spinners.save(true);
                new savePatchCommand(this.patchSaveName(), this.patchDocument(), this.activeDatabase())
                    .execute()
                    .always(() => this.spinners.save(false))
                    .done(() => {
                        this.inSaveMode(false);
                        this.patchSaveName("");
                        this.savePatchValidationGroup.errors.showAllMessages(false);
                        this.savedPatches.loadAll(this.activeDatabase());
                    });
            }
        } else {
            if (this.isValid(this.runPatchValidationGroup)) {
                this.inSaveMode(true);    
            }
        }
    }

    private patchOnDocuments(documentIds: Array<string>) {
        eventsCollector.default.reportEvent("patch", "run", "selected");
        const message = documentIds.length > 1 ? `Are you sure you want to apply this patch to ${documentIds.length} documents?` : 'Are you sure you want to patch document?';

        this.confirmationMessage("Patch", message, ["Cancel", "Patch"])
            .done(result => {
                if (result.can) {
                    const bulkDocs = documentIds.map(docId => ({
                        Id: docId,
                        Type: 'PATCH' as Raven.Client.Documents.Commands.Batches.CommandType,
                        Patch: {
                            Script: this.patchDocument().script()
                        }
                    } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData));

                    new executeBulkDocsCommand(bulkDocs, this.activeDatabase())
                        .execute()
                        .done(() => messagePublisher.reportSuccess("Patch completed"))
                        .fail((result: JQueryXHR) => messagePublisher.reportError("Unable to patch documents.",
                            result.responseText,
                            result.statusText));
                }
            });
    }

    private patchOnIndex() {
        eventsCollector.default.reportEvent("patch", "run", "index");
        const indexToPatch = this.patchDocument().selectedItem();
        const query = this.patchDocument().query();
        const message = `Are you sure you want to apply this patch to matching documents?`;

        this.confirmationMessage("Patch", message, ["Cancel", "Patch all"])
            .done(result => {
                if (result.can) {
                    const patch = {
                        Script: this.patchDocument().script()
                    } as Raven.Server.Documents.Patch.PatchRequest;

                    new patchByQueryCommand(indexToPatch, query, patch, this.activeDatabase())
                        .execute()
                        .done((operationIdDto) => {
                            notificationCenter.instance.openDetailsForOperationById(this.activeDatabase(), operationIdDto.OperationId);
                        });
                }
            });
    }

    private patchOnCollection() {
        eventsCollector.default.reportEvent("patch", "run", "collection");
        const collectionToPatch = this.patchDocument().selectedItem();
        const message = `Are you sure you want to apply this patch to all documents in '${collectionToPatch}' collection?`;

        this.confirmationMessage("Patch", message, ["Cancel", "Patch all"])
            .done(result => {
                if (result.can) {

                    const patch = {
                        Script: this.patchDocument().script()
                    } as Raven.Server.Documents.Patch.PatchRequest;

                    new patchByCollectionCommand(collectionToPatch, patch, this.activeDatabase())
                        .execute()
                        .done((operationIdDto) => {
                            notificationCenter.instance.openDetailsForOperationById(this.activeDatabase(), operationIdDto.OperationId);
                        });
                }
            });
    }

    static fetchDocumentIdAutocomplete(prefix: string, db: database, output: KnockoutObservableArray<string>) {
        if (prefix && prefix.length > 1) {
            new getDocumentsMetadataByIDPrefixCommand(prefix, 10, db)
                .execute()
                .done(result => {
                    output(result.map(x => x["@metadata"]["@id"]));
                });
        } else {
            output([]);
        }
    }

    private fetchAllCollections(): JQueryPromise<collectionsStats> {
        return new getCollectionsStatsCommand(this.activeDatabase())
            .execute()
            .done((stats: collectionsStats) => {
                this.collections(stats.collections);
            });
    }

    private fetchAllIndexes(): JQueryPromise<Raven.Client.Documents.Operations.DatabaseStatistics> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results) => {
                this.indexNames(results.Indexes.filter(x => x.Type === "Map").map(x => x.Name));
            });
    }

    previewDocument() {
        this.spinners.preview(true);

        this.afterAsyncValidationCompleted(this.previewDocumentValidationGroup, () => {
            if (this.isValid(this.previewDocumentValidationGroup)) {
                new getDocumentWithMetadataCommand(this.patchDocument().selectedItem(), this.activeDatabase())
                    .execute()
                    .done((doc: document) => {
                        const docDto = doc.toDto(true);
                        const metaDto = docDto["@metadata"];
                        documentMetadata.filterMetadata(metaDto);
                        const text = JSON.stringify(docDto, null, 4);
                        app.showBootstrapDialog(new showDataDialog("Document: " + doc.getId(), text, "javascript"));
                    })
                    .always(() => this.spinners.preview(false));
            } else {
                this.spinners.preview(false);
            }
        });
    }

    enterTestMode() {
        const patchDoc = this.patchDocument();

        let documentIdToUse: string = null;
        switch (patchDoc.patchOnOption()) {
            case "Document":
                documentIdToUse = patchDoc.selectedItem();
                break;
            case "Collection":
            case "Index":
                const selection = this.gridController().getSelectedItems();
                if (selection.length > 0) {
                    documentIdToUse = selection[0].getId();
                }
                break;
        }

        this.test.enterTestMode(documentIdToUse);
    }
}

export = patch;
