import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/patchDocument");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import patchParam = require("models/patchParam");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import collection = require("models/collection");
import customColumns = require("models/customColumns");
import document = require("models/document");
import pagedList = require("common/pagedList");
import jsonUtil = require("common/jsonUtil");
import queryIndexCommand = require("commands/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import savePatch = require('viewmodels/savePatch');
import loadPatch = require('viewmodels/loadPatch');
import executePatchConfirm = require('viewmodels/executePatchConfirm');
import savePatchCommand = require('commands/savePatchCommand');
import executePatchCommand = require("commands/executePatchCommand");
import virtualTable = require("widgets/virtualTable/viewModel");
import evalByQueryCommand = require("commands/evalByQueryCommand");
import documentMetadata = require("models/documentMetadata");
import getIndexTermsCommand = require("commands/getIndexTermsCommand");
import getDocumentsMetadataByIDPrefixCommand = require("commands/getDocumentsMetadataByIDPrefixCommand");
import getDocumentsByEntityNameCommand = require("commands/getDocumentsByEntityNameCommand");
import pagedResultSet = require("common/pagedResultSet");
import getIndexDefinitionCommand = require("commands/getIndexDefinitionCommand");
import queryUtil = require("common/queryUtil");

class patch extends viewModelBase {

    displayName = "patch";
    indexNames = ko.observableArray<string>([]);
    indexNamesToSelect: KnockoutComputed<string[]>;
    collections = ko.observableArray<collection>([]);
    collectionToSelect: KnockoutComputed<collection[]>;

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

    isExecuteAllowed: KnockoutComputed<boolean>;
    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

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

        this.indexNamesToSelect = ko.computed(() => {
            var indexNames = this.indexNames();
            var patchDocument = this.patchDocument();
            if (indexNames.length === 0 || !patchDocument)
                return [];

            return indexNames.filter(x => x !== patchDocument.selectedItem());
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

    activate(navigationArgs) {
        super.activate(navigationArgs);
        this.updateHelpLink('QGGJR5');
        this.patchDocument(patchDocument.empty());
        this.queryText.throttle(1000).subscribe(v => {
            this.runQuery();
        });

        this.isExecuteAllowed = ko.computed(() => ((this.patchDocument().script()) && (this.beforePatchDoc())) ? true : false);
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
            var firstCheckedOnList = list.last();
            if (firstCheckedOnList != null) {
                this.currentCollectionPagedItems().getNthItem(firstCheckedOnList)
                    .done(document => {
                        this.documentKey(document.__metadata.id);
                        this.beforePatchDoc(JSON.stringify(document.toDto(), null, 4));
                        this.beforePatchMeta(JSON.stringify(documentMetadata.filterMetadata(document.__metadata.toDto()), null, 4));
                    });
            } else {
                this.clearDocumentPreview();
            }
        });

    }

    attached() {
        super.attached();
        $("#indexQueryLabel").popover({
            html: true,
            trigger: 'hover',
            container: '.form-horizontal',
            content: 'Queries use Lucene syntax. Examples:<pre><span class="code-keyword">Name</span>: Hi?berna*<br/><span class="code-keyword">Count</span>: [0 TO 10]<br/><span class="code-keyword">Title</span>: "RavenDb Queries 1010" AND <span class="code-keyword">Price</span>: [10.99 TO *]</pre>',
        });
        $("#patchScriptsLabel").popover({
            html: true,
            trigger: 'hover',
            container: '.form-horizontal',
            content: 'Patch Scripts are written in JavaScript. Examples:<pre><span class="code-keyword">this</span>.NewProperty = <span class="code-keyword">this</span>.OldProperty + myParameter;<br/><span class="code-keyword">delete this</span>.UnwantedProperty;<br/><span class="code-keyword">this</span>.Comments.RemoveWhere(<span class="code-keyword">function</span>(comment){<br/>  <span class="code-keyword">return</span> comment.Spam;<br/>});</pre>',
        });

        var rowCreatedEvent = app.on(patch.gridSelector + 'RowsCreated').then(() => {
            rowCreatedEvent.off();
        });
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
            }).fail(this.clearDocumentPreview());
        } else {
            this.clearDocumentPreview();
        }
    }

    private clearDocumentPreview() {
        this.beforePatchDoc('');
        this.beforePatchMeta('');
        this.afterPatchDoc('');
        this.afterPatchMeta('');
    }

    setSelectedPatchOnOption(patchOnOption: string) {
        this.patchDocument().patchOnOption(patchOnOption);
        this.patchDocument().selectedItem('');
        this.clearDocumentPreview();
        switch (patchOnOption) {
            case "Collection":
                this.fetchAllCollections();
                break;
            case "Index":
                this.fetchAllIndexes();
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
                var currentlySelectedCollection:collection = null;

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
        this.patchDocument().selectedItem(coll.name);
        var list = coll.getDocuments();
        this.currentCollectionPagedItems(list);
        list.fetch(0, 20).always(() => $("#matchingDocumentsGrid").resize());
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => {
                this.indexNames(results.Indexes.map(i => i.Name));
                if (this.indexNames().length > 0) {
                    this.setSelectedIndex(this.indexNames().first());
                }
            });
    }

    setSelectedIndex(indexName: string) {
        this.selectedIndex(indexName);///
        this.patchDocument().selectedItem(indexName);
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
                return command.execute();
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
            new savePatchCommand(patchName, this.patchDocument(), this.activeDatabase()).execute();
        });
    }

    loadPatch() {
        this.clearDocumentPreview();
        var loadPatchViewModel: loadPatch = new loadPatch(this.activeDatabase());
        app.showDialog(loadPatchViewModel);
        loadPatchViewModel.onExit().done((patch) => {
            var selectedItem = patch.selectedItem();
            this.patchDocument(patch.cloneWithoutMetadata());
            switch (this.patchDocument().patchOnOption()) {
                case "Collection":
                    this.fetchAllCollections().then(() => {
                        this.setSelectedCollection(this.collections().filter(coll => (coll.name === selectedItem)).first());
                    });
                    break;
                case "Index":
                    this.fetchAllIndexes().then(() => {
                        this.setSelectedIndex(selectedItem);
                    });
                    break;
                case "Document":
                    this.loadDocumentToTest(patch.selectedItem());
                    break;
            }
        });
    }

    testPatch() {
        var values = {};
        var patchDtos = this.patchDocument().parameters().map(param => {
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

        var patchDtos = this.patchDocument().parameters().map(param => {
            var dto = param.toDto();
            values[dto.Key] = dto.Value;
        });

        var patch = {
            Script: this.patchDocument().script(),
            Values: values
        };

        new evalByQueryCommand(index, query, JSON.stringify(patch), this.activeDatabase()).execute();
    }

    private confirmAndExecutePatch(keys: string[]) {
        var confirmExec = new executePatchConfirm();
        confirmExec.viewTask.done(() => this.executePatch(keys));
        app.showDialog(confirmExec);
    }

    private executePatch(keys: string[]) {
        var values = {};
        var patchDtos = this.patchDocument().parameters().map(param => {
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
            .fail((result: JQueryXHR) => console.log(result.responseText))
    }

    private updateDocumentsList() {
        switch (this.patchDocument().patchOnOption()) {
            case "Collection":
                this.fetchAllCollections().then(() => {
                    this.setSelectedCollection(this.collections().filter(coll => (coll.name === this.patchDocument().selectedItem())).first());
                });
                break;
            case "Index":
                this.setSelectedIndex(this.patchDocument().selectedItem());
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
                });
            }
        }
    }
}

export = patch;
