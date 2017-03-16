import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import collectionsStats = require("models/database/documents/collectionsStats");
import getCollectionsStatsCommand = require("commands/database/documents/getCollectionsStatsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import jsonUtil = require("common/jsonUtil");
import messagePublisher = require("common/messagePublisher");
import appUrl = require("common/appUrl");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/database/documents/getDocumentsMetadataByIDPrefixCommand");
import savePatch = require('viewmodels/database/patch/savePatch');
import savePatchCommand = require('commands/database/patch/savePatchCommand');
import patchByQueryCommand = require("commands/database/patch/patchByQueryCommand");
import patchByCollectionCommand = require("commands/database/patch/patchByCollectionCommand");
import documentMetadata = require("models/database/documents/documentMetadata");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import queryUtil = require("common/queryUtil");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');
import killOperationComamnd = require('commands/operations/killOperationCommand');
import eventsCollector = require("common/eventsCollector");
import notificationCenter = require("common/notifications/notificationCenter");
import genUtils = require("common/generalUtils");
import queryCriteria = require("models/database/query/queryCriteria");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import documentBasedColumnsProvider = require("widgets/virtualGrid/columns/providers/documentBasedColumnsProvider");
import executeBulkDocsCommand = require("commands/database/documents/executeBulkDocsCommand");

type fetcherType = (skip: number, take: number) => JQueryPromise<pagedResult<document>>;

class patch extends viewModelBase {

    gridController = ko.observable<virtualGridController<document>>(); //TODO: column preview, custom columns?
    private documentsProvider: documentBasedColumnsProvider;
    private fetcher = ko.observable<fetcherType>();

    patchDocument = ko.observable<patchDocument>(patchDocument.empty());

    indexNames = ko.observableArray<string>();
    indexFields = ko.observableArray<string>(); //TODO: fetch me!
    collections = ko.observableArray<collection>([]);

    isDocumentMode: KnockoutComputed<boolean>;
    isCollectionMode: KnockoutComputed<boolean>;
    isIndexMode: KnockoutComputed<boolean>;

    documentIdSearchResults = ko.observableArray<string>();

    runPatchValidationGroup: KnockoutValidationGroup;
    runQueryValidationGroup: KnockoutValidationGroup;

    //TODO: implement: Data has changed. Your results may contain duplicates or non-current entries

    constructor() {
        super();
        aceEditorBindingHandler.install();

        this.initValidation();

        this.bindToCurrentInstance("usePatchOption", "useIndex", "useCollection");
        this.initObservables();
    }

    private initValidation() {
        const doc = this.patchDocument();

        doc.script.extend({
            required: true
        });

        doc.selectedItem.extend({
            required: true
        });

        this.runPatchValidationGroup = ko.validatedObservable({
            script: doc.script,
            selectedItem: doc.selectedItem
        });
        this.runQueryValidationGroup = ko.validatedObservable({
            selectedItem: doc.selectedItem
        });
    }

    private initObservables() {
        this.isDocumentMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Document");
        this.isCollectionMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Collection");
        this.isIndexMode = ko.pureComputed(() => this.patchDocument().patchOnOption() === "Index");

        this.patchDocument().selectedItem.throttle(250).subscribe(item => {
            if (this.patchDocument().patchOnOption() === "Document") {
                this.fetchDocumentIdAutocomplete(item);
            }
        });

        this.patchDocument().patchAll.subscribe((patchAll) => {
            this.documentsProvider.showRowSelectionCheckbox = !patchAll;
            this.gridController().reset(true);
        });
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");

        //TODO: fetch all patches

        return $.when<any>(this.fetchAllCollections(), this.fetchAllIndexes());
    }

    attached() {
        super.attached();

        //TODO: put those on UI

        $("#indexQueryLabel").popover({
            html: true,
            trigger: "hover",
            container: '.form-horizontal', //TODO: verify
            content: '<p>Queries use Lucene syntax. Examples:</p><pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>'
        });
        $("#patchScriptsLabel").popover({
            html: true,
            trigger: "hover",
            container: ".form-horizontal", //TODO: verify
            content: '<p>Patch Scripts are written in JavaScript. Examples:</p><pre><span class="code-keyword">this</span>.NewProperty = <span class="code-keyword">this</span>.OldProperty + myParameter;<br/><span class="code-keyword">delete this</span>.UnwantedProperty;<br/><span class="code-keyword">this</span>.Comments.RemoveWhere(<span class="code-keyword">function</span>(comment){<br/>  <span class="code-keyword">return</span> comment.Spam;<br/>});</pre>'
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
            totalResultCount: 0
        });

        grid.headerVisible(true);
        grid.init((s, t) => this.fetcher() ? this.fetcher()(s, t) : fakeFetcher(s, t), (w, r) => this.documentsProvider.findColumns(w, r));

        this.fetcher.subscribe(() => grid.reset());
    }

    usePatchOption(option: patchOption) {
        this.fetcher(null);

        const patchDoc = this.patchDocument();
        patchDoc.patchOnOption(option);
        patchDoc.selectedItem(null);
        patchDoc.patchAll(option === "Index" || option === "Collection");

        this.runPatchValidationGroup.errors.showAllMessages(false);
    }

    useIndex(indexName: string) {
        const patchDoc = this.patchDocument();
        patchDoc.selectedItem(indexName);
        patchDoc.patchAll(true);

        queryUtil.fetchIndexFields(this.activeDatabase(), indexName, this.indexFields);

        this.runQuery();
    }

    useCollection(collectionToUse: collection) {
        const fetcher = (skip: number, take: number) => collectionToUse.fetchDocuments(skip, take);
        this.fetcher(fetcher);

        const patchDoc = this.patchDocument();
        patchDoc.selectedItem(collectionToUse.name);
        patchDoc.patchAll(true);
    }

    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {
        queryUtil.queryCompleter(this.indexFields, this.patchDocument().selectedIndex, this.activeDatabase, editor, session, pos, prefix, callback);
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

    private patchOnDocuments(documentIds: Array<string>) {
        eventsCollector.default.reportEvent("patch", "run", "selected");
        const message = documentIds.length > 1 ? `Are you sure you want to apply this patch to ${documentIds.length} documents?` : 'Are you sure you want to patch document?';

        this.confirmationMessage("Patch", message, ["Cancel", "Patch"])
            .done(result => {
                if (result.can) {
                    const bulkDocs = documentIds.map(docId => ({
                        Key: docId,
                        Method: 'PATCH' as Raven.Server.Documents.Handlers.CommandType,
                        Patch: {
                            Script: this.patchDocument().script()
                        }
                    } as Raven.Server.Documents.Handlers.CommandData));

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

    private fetchDocumentIdAutocomplete(prefix: string) {
        if (prefix && prefix.length > 1) {
            new getDocumentsMetadataByIDPrefixCommand(prefix, 10, this.activeDatabase())
                .execute()
                .done(result => {
                    this.documentIdSearchResults(result.map(x => x["@metadata"]["@id"]));
                });
        } else {
            this.documentIdSearchResults([]);
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

    /* TODO:
    
    savedPatches = ko.observableArray<patchDocument>();

    showDocumentsPreview: KnockoutObservable<boolean>;

    beforePatch: KnockoutComputed<string>;
    beforePatchDoc = ko.observable<string>();
    beforePatchMeta = ko.observable<string>();
    beforePatchDocMode = ko.observable<boolean>(true);
    beforePatchEditor: AceAjax.Editor;

    afterPatch = ko.observable<string>();
    afterPatchDoc = ko.observable<string>();
    afterPatchMeta = ko.observable<string>();
    afterPatchDocMode = ko.observable<boolean>(true);
    afterPatchEditor: AceAjax.Editor;

    loadedDocuments = ko.observableArray<string>();
    putDocuments = ko.observableArray<any>();
    outputLog = ko.observableArray<string>();

    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

    constructor() {
        super();

        // When we programmatically change the document text or meta text, push it into the editor.
        this.beforePatchDocMode.subscribe(() => {
            if (this.beforePatchEditor) {
                var text = this.beforePatchDocMode() ? this.beforePatchDoc() : this.beforePatchMeta();
                this.beforePatchEditor.getSession().setValue(text);
            }
        });
        this.beforePatch = ko.computed({
            read: () => {
                return this.beforePatchDocMode() ? this.beforePatchDoc() : this.beforePatchMeta();
            },
            write: (text: string) => {
                var currentObservable = this.beforePatchDocMode() ? this.beforePatchDoc : this.beforePatchMeta;
                currentObservable(text);
            },
            owner: this
        });

        this.afterPatchDocMode.subscribe(() => {
            if (this.afterPatchEditor) {
                var text = this.afterPatchDocMode() ? this.afterPatchDoc() : this.afterPatchMeta();
                this.afterPatchEditor.getSession().setValue(text);
            }
        });
        this.afterPatch = ko.computed({
            read: () => {
                return this.afterPatchDocMode() ? this.afterPatchDoc() : this.afterPatchMeta();
            },
            write: (text: string) => {
                var currentObservable = this.afterPatchDocMode() ? this.afterPatchDoc : this.afterPatchMeta;
                currentObservable(text);
            },
            owner: this
        });

        // Refetch the index fields whenever the selected index name changes.
        this.selectedIndex
            .subscribe(indexName => {
                if (indexName) {
                    this.fetchIndexFields(indexName);
                }
            });

        this.indicesToSelect = ko.computed(() => {
            var indicies = this.indices();
            var patchDocument = this.patchDocument();
            if (indicies.length === 0 || !patchDocument)
                return [];

            return indicies.filter(x => x.name !== patchDocument.selectedItem());
        });

        this.collectionToSelect = ko.computed(() => {
            var collections = this.collections();
            var patchDocument = this.patchDocument();
            if (collections.length === 0 || !patchDocument)
                return [];

            return collections.filter((x: collection) => x.name !== patchDocument.selectedItem());
        });

        this.showDocumentsPreview = ko.computed(() => {
            if (!this.patchDocument()) {
                return false;
            }
            var indexPath = this.patchDocument().isIndexPatch();
            var collectionPath = this.patchDocument().isCollectionPatch();
            return indexPath || collectionPath;
        });
    }

    compositionComplete() {
        super.compositionComplete();

        var beforePatchEditorElement = $("#beforePatchEditor");
        if (beforePatchEditorElement.length > 0) {
            this.beforePatchEditor = ko.utils.domData.get(beforePatchEditorElement[0], "aceEditor");
        }

        var afterPatchEditorElement = $("#afterPatchEditor");
        if (afterPatchEditorElement.length > 0) {
            this.afterPatchEditor = ko.utils.domData.get(afterPatchEditorElement[0], "aceEditor");
        }
       

        grid.selection.subscribe(selection => {
            if (selection.count === 1) {
                var document = selection.included[0];
                // load document directly from server as documents on list are loaded using doc-preview endpoint, which doesn't display entire document
                this.loadDocumentToTest(document.__metadata.id);
                this.documentKey(document.__metadata.id);
            } else {
                this.clearDocumentPreview();
            }
        });

        //TODO: install doc preview tooltip
    }

    activate(recentPatchHash?: string) {

        this.isExecuteAllowed = ko.computed(() => !!this.patchDocument().script() && !!this.beforePatchDoc());
        this.keyOfTestedDocument = ko.computed(() => {
            switch (this.patchDocument().patchOnOption()) {
                case "Collection":
                case "Index":
                    return this.documentKey();
                case "Document":
                    return this.patchDocument().selectedItem();
            }
        });

        this.fetchAllPatches();

        if (recentPatchHash) {
            this.selectInitialPatch(recentPatchHash);
        }
    }

    private fetchAllPatches() {
        new getPatchesCommand(this.activeDatabase())
            .execute()
            .done((patches: patchDocument[]) => this.savedPatches(patches));
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    loadDocumentToTest(selectedItem: string) {
        if (selectedItem) {
            var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
            loadDocTask.done(document => {
                this.beforePatchDoc(JSON.stringify(document.toDto(), null, 4));
                this.beforePatchMeta(JSON.stringify(documentMetadata.filterMetadata(document.__metadata.toDto()), null, 4));
            }).fail(() => this.clearDocumentPreview());
        } else {
            this.clearDocumentPreview();
        }
    }

    private clearDocumentPreview() {
        this.beforePatchDoc("");
        this.beforePatchMeta("");
        this.afterPatchDoc("");
        this.afterPatchMeta("");
        this.putDocuments([]);
        this.loadedDocuments([]);
        this.outputLog([]);
    }


    savePatch() {
        eventsCollector.default.reportEvent("patch", "save");

        var savePatchViewModel: savePatch = new savePatch();
        app.showBootstrapDialog(savePatchViewModel);
        savePatchViewModel.onExit().done((patchName) => {
            new savePatchCommand(patchName, this.patchDocument(), this.activeDatabase())
                .execute()
                .done(() => this.fetchAllPatches());
        });
    }

    private usePatch(patch: patchDocument) {
        this.clearDocumentPreview();
        var selectedItem = patch.selectedItem();
        patch = patch.clone();
        patch.resetMetadata();
        this.patchDocument(patch);
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === selectedItem))[0]);
                });
                break;
            case "Index":
                this.fetchAllIndexes().then(() => {
                    this.setSelectedIndex(selectedItem);
                    this.queryText(patch.query());
                    this.runQuery();
                });
                break;
            case "Document":
                this.loadDocumentToTest(patch.selectedItem());
                break;
        }
    }

    testPatch() {
        eventsCollector.default.reportEvent("patch", "test");

        var values: dictionary<string> = {};
        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });
        var bulkDocs: Array<Raven.Server.Documents.Handlers.CommandData> = [];
        bulkDocs.push({
            Key: this.keyOfTestedDocument(),
            Method: 'PATCH',
            DebugMode: true,
            Patch: {
                Script: this.patchDocument().script(),
                Values: values
            }
        });
        new executePatchCommand(bulkDocs, this.activeDatabase(), true)
            .execute()
            .done((result: Raven.Server.Documents.Handlers.CommandData[]) => {
                var testResult = new document((<any>result).Results[0].AdditionalData['Document']);
                this.afterPatchDoc(JSON.stringify(testResult.toDto(), null, 4));
                this.afterPatchMeta(JSON.stringify(documentMetadata.filterMetadata(testResult.__metadata.toDto()), null, 4));
                this.updateActions((<any>result).Results[0].AdditionalData['Actions']);
                this.outputLog((<any>result).Results[0].AdditionalData["Debug"]);
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));
        this.recordPatchRun();
    }

    private updatePageUrl(hash: number) {
        // Put the patch into the URL, so that if the user refreshes the page, he's still got this patch loaded.
        var queryUrl = appUrl.forPatch(this.activeDatabase(), hash);
        this.updateUrl(queryUrl);
    }

    private updateActions(actions: { PutDocument: any[]; LoadDocument: any }) {
        this.loadedDocuments(actions.LoadDocument || []);
        this.putDocuments((actions.PutDocument || []).map(doc => jsonUtil.syntaxHighlight(doc)));
    }

    private updateDocumentsList() {
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === this.patchDocument().selectedItem()))[0]);
                });
                break;
            case "Index":
                this.useIndex(this.patchDocument().selectedItem());
                break;
        }
    }

    activateBeforeDoc() {
        this.beforePatchDocMode(true);
    }

    activateBeforeMeta() {
        this.beforePatchDocMode(false);
    }

    activateAfterDoc() {
        this.afterPatchDocMode(true);
    }

    activateAfterMeta() {
        this.afterPatchDocMode(false);
    }

    selectedIndex = ko.observable<string>();
    isTestIndex = ko.observable<boolean>(false);

   
    }*/
}

export = patch;
