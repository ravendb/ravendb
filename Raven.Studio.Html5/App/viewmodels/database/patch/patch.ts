import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/database/patch/patchDocument");
import aceEditorBindingHandler = require("common/bindingHelpers/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getCollectionsCommand = require("commands/database/documents/getCollectionsCommand");
import collection = require("models/database/documents/collection");
import document = require("models/database/documents/document");
import pagedList = require("common/pagedList");
import jsonUtil = require("common/jsonUtil");
import appUrl = require("common/appUrl");
import queryIndexCommand = require("commands/database/query/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import savePatch = require('viewmodels/database/patch/savePatch');
import executePatchConfirm = require('viewmodels/database/patch/executePatchConfirm');
import savePatchCommand = require('commands/database/patch/savePatchCommand');
import executePatchCommand = require("commands/database/patch/executePatchCommand");
import virtualTable = require("widgets/virtualTable/viewModel");
import evalByQueryCommand = require("commands/database/patch/evalByQueryCommand");
import documentMetadata = require("models/database/documents/documentMetadata");
import getDocumentsByEntityNameCommand = require("commands/database/documents/getDocumentsByEntityNameCommand");
import pagedResultSet = require("common/pagedResultSet");
import getIndexDefinitionCommand = require("commands/database/index/getIndexDefinitionCommand");
import queryUtil = require("common/queryUtil");
import recentPatchesStorage = require("common/recentPatchesStorage");
import getPatchesCommand = require('commands/database/patch/getPatchesCommand');
import killRunningTaskCommand = require('commands/operations/killRunningTaskCommand');
import getRunningTasksCommand = require("commands/operations/getRunningTasksCommand");

type indexInfo = {
    name: string;
    isMapReduce: boolean;
}

class patch extends viewModelBase {

    displayName = "patch";
    indices = ko.observableArray<indexInfo>([]);
    indicesToSelect: KnockoutComputed<indexInfo[]>;
    collections = ko.observableArray<collection>([]);
    collectionToSelect: KnockoutComputed<collection[]>;

    recentPatches = ko.observableArray<storedPatchDto>();
    savedPatches = ko.observableArray<patchDocument>();

    currentCollectionPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();
    showDocumentsPreview: KnockoutObservable<boolean>;

    patchDocument = ko.observable<patchDocument>();

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

    runningPatchesCount = ko.observable<number>();
    runningTasksUrl = ko.computed(() => appUrl.forRunningTasks(this.activeDatabase()));
    runningPatchesText = ko.computed(() => {
        var count = this.runningPatchesCount();
        if (count > 1) {
            return count + ' patches in progress';
        }
        return count + ' patch in progress';
    });
    runningPatchesPollingHandle: number;

    isExecuteAllowed: KnockoutComputed<boolean>;
    isMapReduceIndexSelected: KnockoutComputed<boolean>;
    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

    isPatchingInProgress = ko.observable<boolean>(false);
    showPatchingProgress = ko.observable<boolean>(false);
    patchOperationId = ko.observable<number>();
    patchingProgress = ko.observable<number>(0);
    patchingProgressPercentage: KnockoutComputed<string>;
    patchingProgressText = ko.observable<string>();
    patchSuccess = ko.observable<boolean>(false);
    patchFailure = ko.observable<boolean>(false);
    patchKillInProgress = ko.observable<boolean>(false);

    static gridSelector = "#matchingDocumentsGrid";

    constructor() {
        super();

        aceEditorBindingHandler.install();

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
            .where(indexName => indexName != null)
            .subscribe(indexName => this.fetchIndexFields(indexName));

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
    }

    activate(recentPatchHash?: string) {
        super.activate(recentPatchHash);
        this.updateHelpLink("QGGJR5");
        this.patchDocument(patchDocument.empty());
        this.queryText.throttle(1000).subscribe(() => {
            this.runQuery();
        });

        this.isExecuteAllowed = ko.computed(() => !!this.patchDocument().script() && !!this.beforePatchDoc());
        this.isMapReduceIndexSelected = ko.computed(() => {
            if (this.patchDocument().patchOnOption() !== "Index") {
                return false;
            }
            var indexName = this.selectedIndex();
            var usedIndex = this.indices().first(x => x.name === indexName);
            if (usedIndex) { 
                return usedIndex.isMapReduce;
            }
            return false;
        })
        this.keyOfTestedDocument = ko.computed(() => {
            switch (this.patchDocument().patchOnOption()) {
                case "Collection":
                case "Index":
                    return this.documentKey();
                case "Document":
                    return this.patchDocument().selectedItem();
            }
        });

        this.selectedDocumentIndices.subscribe(list => {
            if (list.length === 1) {
                var firstCheckedOnList = list.first();
                this.currentCollectionPagedItems().getNthItem(firstCheckedOnList)
                    .done(document => {
                        // load document directly from server as documents on list are loaded using doc-preview endpoint, which doesn't display entire document
                        this.loadDocumentToTest(document.__metadata.id);
                        this.documentKey(document.__metadata.id);
                    });
            } else {
                this.clearDocumentPreview();
            }
        });

        this.patchingProgressPercentage = ko.computed(() => this.patchingProgress() + "%");

        var db = this.activeDatabase();
        if (!!db) {
            this.fetchRecentPatches();
            this.fetchAllPatches();
            this.fetchRunningPatches();
        }

        if (recentPatchHash) {
            this.selectInitialPatch(recentPatchHash);
        }
    }

    attached() {
        super.attached();
        $("#indexQueryLabel").popover({
            html: true,
            trigger: "hover",
            container: '.form-horizontal',
            content: '<p>Queries use Lucene syntax. Examples:</p><pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>'
        });
        $("#patchScriptsLabel").popover({
            html: true,
            trigger: "hover",
            container: ".form-horizontal",
            content: '<p>Patch Scripts are written in JavaScript. Examples:</p><pre><span class="code-keyword">this</span>.NewProperty = <span class="code-keyword">this</span>.OldProperty + myParameter;<br/><span class="code-keyword">delete this</span>.UnwantedProperty;<br/><span class="code-keyword">this</span>.Comments.RemoveWhere(<span class="code-keyword">function</span>(comment){<br/>  <span class="code-keyword">return</span> comment.Spam;<br/>});</pre>'
        });

        var rowCreatedEvent = app.on(patch.gridSelector + 'RowsCreated').then(() => {
            rowCreatedEvent.off();
        });

        var self = this;
        $(window).bind('storage', () => {
            self.fetchRecentPatches();
        });
    }

    private fetchAllPatches() {
        new getPatchesCommand(this.activeDatabase())
            .execute()
            .done((patches: patchDocument[]) => this.savedPatches(patches));
    }

    private fetchRunningPatches() {
        if (this.runningPatchesPollingHandle)
            return;

        new getRunningTasksCommand(this.activeDatabase())
            .execute()
            .done((tasks: runningTaskDto[]) => {
                var count = tasks.filter(x => x.TaskType === "IndexBulkOperation" && !x.Completed).length;
                this.runningPatchesCount(count);

                // we enable polling only if at least one patch is in progress
                if (count > 0) {
                    this.runningPatchesPollingHandle = setTimeout(() => {
                        this.runningPatchesPollingHandle = null;
                        this.fetchRunningPatches();
                    }, 5000);
                } else {
                    this.runningPatchesPollingHandle = null;
                }
            });
    }

    private fetchRecentPatches() {
        this.recentPatches(recentPatchesStorage.getRecentPatches(this.activeDatabase()));
    }

    detached() {
        super.detached();
        aceEditorBindingHandler.detached();
    }

    selectInitialPatch(recentPatchHash: string) {
        if (recentPatchHash.indexOf("recentpatch-") === 0) {
            var hash = parseInt(recentPatchHash.substr("recentpatch-".length), 10);
            var matchingPatch = this.recentPatches.first(q => q.Hash === hash);
            if (matchingPatch) {
                this.useRecentPatch(matchingPatch);
            } else {
                this.navigate(appUrl.forPatch(this.activeDatabase()));
            }
        }
    }

    loadDocumentToTest(selectedItem: string) {
        if (selectedItem) {
            var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
            loadDocTask.done(document => {
                this.beforePatchDoc(JSON.stringify(document.toDto(), null, 4));
                this.beforePatchMeta(JSON.stringify(documentMetadata.filterMetadata(document.__metadata.toDto()), null, 4));
            }).fail(this.clearDocumentPreview());
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

    setSelectedPatchOnOption(patchOnOption: string) {
        this.resetProgressBar();
        this.patchDocument().patchOnOption(patchOnOption);
        this.patchDocument().selectedItem('');
        this.clearDocumentPreview();
        switch (patchOnOption) {
            case "Collection":
                this.fetchAllCollections();
                break;
            case "Index":
                this.fetchAllIndexes()
                    .done(() => this.runQuery());
                $("#matchingDocumentsGrid").resize();
                break;
            default:
                this.currentCollectionPagedItems(null);
                break;
        }
    }

    fetchAllCollections(): JQueryPromise<any> {
        return new getCollectionsCommand(this.activeDatabase())
            .execute()
            .always(() => NProgress.done())
            .done((colls: collection[]) => {
                var currentlySelectedCollection: collection = null;

                if (this.patchDocument().selectedItem()) {
                    var selected = this.patchDocument().selectedItem();
                    currentlySelectedCollection = this.collections.first(c => c.name === selected);
                }

                this.collections(colls);
                if (this.collections().length > 0) {
                    this.setSelectedCollection(currentlySelectedCollection || this.collections().first());
                }
            });
    }

    setSelectedCollection(coll: collection) {
        this.resetProgressBar();
        this.patchDocument().selectedItem(coll.name);
        var list = coll.getDocuments();
        this.currentCollectionPagedItems(list);
        list.fetch(0, 20).always(() => $("#matchingDocumentsGrid").resize());
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => {
                this.indices(results.Indexes.map(i => {
                    return {
                        name: i.Name,
                        isMapReduce: i.IsMapReduce
                    }
                }));
                if (this.indices().length > 0) {
                    this.setSelectedIndex(this.indices().first().name);
                }
            });
    }

    setSelectedIndex(indexName: string) {
        this.resetProgressBar();
        this.selectedIndex(indexName);
        this.patchDocument().selectedItem(indexName);
    }

    useIndex(indexName: string) {
        this.setSelectedIndex(indexName);
        this.runQuery();
    }

    runQuery(): pagedList {
        var selectedIndex = this.patchDocument().selectedItem();
        if (selectedIndex) {
            var queryText = this.queryText();
            this.patchDocument().query(queryText);
            var database = this.activeDatabase();
            var resultsFetcher = (skip: number, take: number) => {
                var command = new queryIndexCommand(selectedIndex, database, skip, take, queryText, []);
                return command.execute()
                    .fail(() => recentPatchesStorage.removeIndexFromRecentPatches(database, selectedIndex));
            };
            var resultsList = new pagedList(resultsFetcher);
            this.currentCollectionPagedItems(resultsList);
            return resultsList;
        }
        return null;
    }

    savePatch() {
        var savePatchViewModel: savePatch = new savePatch();
        app.showDialog(savePatchViewModel);
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
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === selectedItem)).first());
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
        var values = {};
        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });
        var bulkDocs: Array<bulkDocumentDto> = [];
        bulkDocs.push({
            Key: this.keyOfTestedDocument(),
            Method: 'EVAL',
            DebugMode: true,
            Patch: {
                Script: this.patchDocument().script(),
                Values: values
            }
        });
        new executePatchCommand(bulkDocs, this.activeDatabase(), true)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                var testResult = new document(result[0].AdditionalData['Document']);
                this.afterPatchDoc(JSON.stringify(testResult.toDto(), null, 4));
                this.afterPatchMeta(JSON.stringify(documentMetadata.filterMetadata(testResult.__metadata.toDto()), null, 4));
                this.updateActions(result[0].AdditionalData['Actions']);
                this.outputLog(result[0].AdditionalData["Debug"]);
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));
        this.recordPatchRun();
    }

    private updatePageUrl(hash: number) {
        // Put the patch into the URL, so that if the user refreshes the page, he's still got this patch loaded.
        var queryUrl = appUrl.forPatch(this.activeDatabase(), hash);
        this.updateUrl(queryUrl);
    }

    recordPatchRun() {
        var patchDocument = this.patchDocument();

        var newPatch = <storedPatchDto>patchDocument.toDto();
        delete newPatch["@metadata"];
        newPatch.Hash = 0;

        var stringForHash = newPatch.PatchOnOption + newPatch.SelectedItem + newPatch.Script + newPatch.Values;

        if (patchDocument.patchOnOption() === "Index") {
            newPatch.Query = this.queryText();
            stringForHash += newPatch.Query;
        }

        newPatch.Hash = stringForHash.hashCode();

        this.updatePageUrl(newPatch.Hash);

        // Add this query to our recent patches list in the UI, or move it to the top of the list if it's already there.
        var existing = this.recentPatches.first(q => q.Hash === newPatch.Hash);
        if (existing) {
            this.recentPatches.remove(existing);
            this.recentPatches.unshift(existing);
        } else {
            this.recentPatches.unshift(newPatch);
        }

        // Limit us to 15 recent patchs
        if (this.recentPatches().length > 15) {
            this.recentPatches.remove(this.recentPatches()[15]);
        }

        //save the recent queries to local storage
        recentPatchesStorage.saveRecentPatches(this.activeDatabase(), this.recentPatches());
    }

    useRecentPatch(patchToUse: storedPatchDto) {
        var patchDoc = new patchDocument(patchToUse);
        this.usePatch(patchDoc);
    }

    private updateActions(actions: { PutDocument: any[]; LoadDocument: any }) {
        this.loadedDocuments(actions.LoadDocument || []);
        this.putDocuments((actions.PutDocument || []).map(doc => jsonUtil.syntaxHighlight(doc)));
    }

    executePatchOnSingle() {
        var keys = [];
        keys.push(this.patchDocument().selectedItem());
        this.confirmAndExecutePatch(keys);
    }

    executePatchOnSelected() {
        this.confirmAndExecutePatch(this.getDocumentsGrid().getSelectedItems().map(doc => doc.__metadata.id));
    }

    executePatchOnAll() {
        var confirmExec = new executePatchConfirm();
        confirmExec.viewTask.done(() => this.executePatchByIndex());
        app.showDialog(confirmExec);
    }

    private executePatchByIndex() {
        var index = null;
        var query = null;
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                index = "Raven/DocumentsByEntityName";
                query = "Tag:" + queryUtil.escapeTerm(this.patchDocument().selectedItem());
                break;
            case "Index":
                index = this.patchDocument().selectedItem();
                query = this.patchDocument().query();
                break;
        }

        var values = {};

        this.patchSuccess(false);
        this.patchFailure(false);

        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });

        var patch = {
            Script: this.patchDocument().script(),
            Values: values
        };

        var patchByQueryCommand = new evalByQueryCommand(index, query, JSON.stringify(patch), this.activeDatabase(), status => this.updateProgress(status));

        patchByQueryCommand.execute()
            .done(() => {
                this.resetProgressBar();
                this.isPatchingInProgress(true);
                this.showPatchingProgress(true);
                this.fetchRunningPatches();
            });

        patchByQueryCommand.getPatchOperationId()
            .done(operationId => this.patchOperationId(operationId));

        patchByQueryCommand.getPatchCompletedTask()
            .always(() => {
                this.patchOperationId(null);
            });

        this.recordPatchRun();
    }

    private resetProgressBar() {
        this.showPatchingProgress(false);     
        this.patchingProgress(0);
        this.patchingProgressText("");
    }

    private updateProgress(status: bulkOperationStatusDto) {

        if (status.OperationProgress != null) {
            var progressValue = Math.round(100 * (status.OperationProgress.ProcessedEntries / status.OperationProgress.TotalEntries));
            this.patchingProgress(progressValue);
            var progressPrefix = "";
            if (status.Completed) {
                if (status.Canceled) {
                    progressPrefix = "Patch canceled: ";
                    this.patchFailure(true);
                } else if (status.Faulted) {
                    progressPrefix = "Patch failed: ";
                    this.patchFailure(true);
                } else {
                    progressPrefix = "Patch completed: ";
                    this.patchSuccess(true);
                }
            }
            if (status.OperationProgress.TotalEntries) {
                this.patchingProgressText(progressPrefix + status.OperationProgress.ProcessedEntries.toLocaleString() + " / " + status.OperationProgress.TotalEntries.toLocaleString() + " (" + progressValue + "%)");    
            }
        }

        if (status.Completed) {
            this.patchKillInProgress(false);
            this.isPatchingInProgress(false);
        }
    }

    private confirmAndExecutePatch(keys: string[]) {
        var confirmExec = new executePatchConfirm();
        confirmExec.viewTask.done(() => this.executePatch(keys));
        app.showDialog(confirmExec);
    }

    private executePatch(keys: string[]) {
        var values = {};
        this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });
        var bulkDocs: Array<bulkDocumentDto> = [];
        keys.forEach(
            key => bulkDocs.push({
                Key: key,
                Method: 'EVAL',
                DebugMode: false,
                Patch: {
                    Script: this.patchDocument().script(),
                    Values: values
                }
            })
        );
        new executePatchCommand(bulkDocs, this.activeDatabase(), false)
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.afterPatchDoc("");
                this.afterPatchMeta("");
                if (this.patchDocument().patchOnOption() === 'Document') {
                    this.loadDocumentToTest(this.patchDocument().selectedItem());
                }
                this.updateDocumentsList();
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));

        this.recordPatchRun();
    }

    private updateDocumentsList() {
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === this.patchDocument().selectedItem())).first());
                });
                break;
            case "Index":
                this.useIndex(this.patchDocument().selectedItem());
                break;
        }
    }

    private getDocumentsGrid(): virtualTable {
        var gridContents = $(patch.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
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

    indexFields = ko.observableArray<string>();
    selectedIndex = ko.observable<string>();
    dynamicPrefix = "dynamic/";
    isTestIndex = ko.observable<boolean>(false);
    queryText = ko.observable("");

    queryCompleter(editor: any, session: any, pos: AceAjax.Position, prefix: string, callback: (errors: any[], worldlist: { name: string; value: string; score: number; meta: string }[]) => void) {

        queryUtil.queryCompleter(this.indexFields, this.selectedIndex, this.dynamicPrefix, this.activeDatabase, editor, session, pos, prefix, callback);

    }

    killPatch() {
        var operationToKill = this.patchOperationId();
        if (operationToKill) {
            this.confirmationMessage("Are you sure?", "You are stopping patch execution.")
                .done(() => {
                    if (this.patchOperationId()) {
                        new killRunningTaskCommand(this.activeDatabase(), operationToKill)
                            .execute()
                            .done(() => {
                                if (this.patchOperationId()) {
                                    this.patchKillInProgress(true);
                                }
                            });
                    }
                });
        }
    }

    fetchIndexFields(indexName: string) {
        // Fetch the index definition so that we get an updated list of fields to be used as sort by options.
        // Fields don't show for All Documents.
        var self = this;
        var isAllDocumentsDynamicQuery = indexName === "All Documents";
        if (!isAllDocumentsDynamicQuery) {
            //if index is dynamic, get columns using index definition, else get it using first index result
            if (indexName.indexOf(this.dynamicPrefix) === 0) {
                var collectionName = indexName.substring(8);
                new getDocumentsByEntityNameCommand(new collection(collectionName, this.activeDatabase()), 0, 1)
                    .execute()
                    .done((result: pagedResultSet) => {
                        if (!!result && result.totalResultCount > 0 && result.items.length > 0) {
                            var dynamicIndexPattern: document = new document(result.items[0]);
                            if (!!dynamicIndexPattern) {
                                this.indexFields(dynamicIndexPattern.getDocumentPropertyNames());
                            }
                        }
                    });
            } else {
                new getIndexDefinitionCommand(indexName, this.activeDatabase())
                    .execute()
                    .done((result: indexDefinitionContainerDto) => {
                        self.isTestIndex(result.Index.IsTestIndex);
                        self.indexFields(result.Index.Fields);
                    })
                    .fail(() => {
                        recentPatchesStorage.removeIndexFromRecentPatches(this.activeDatabase(), indexName);
                    });
            }
        }
    }
}

export = patch;
