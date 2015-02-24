import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import patchDocument = require("models/patchDocument");
import aceEditorBindingHandler = require("common/aceEditorBindingHandler");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getCollectionsCommand = require("commands/getCollectionsCommand");
import collection = require("models/collection");
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

class patch extends viewModelBase {

    displayName = "patch";
    indexNames = ko.observableArray<string>();
    collections = ko.observableArray<collection>();

    currentCollectionPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    patchDocument = ko.observable<patchDocument>();

    beforePatch = ko.observable<string>();
    afterPatch = ko.observable<string>();

    loadedDocuments = ko.observableArray<string>();
    putDocuments = ko.observableArray<any>();

    isExecuteAllowed: KnockoutComputed<boolean>;
    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

    static gridSelector = "#matchingDocumentsGrid";

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate(navigationArgs) {
        super.activate(navigationArgs);
        this.updateHelpLink('QGGJR5');
        this.patchDocument(patchDocument.empty());
        this.isExecuteAllowed = ko.computed(() => ((this.patchDocument().script()) && (this.beforePatch())) ? true : false);
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
                        this.beforePatch(JSON.stringify(document.toDto(), null, 4));
                    });
            } else {
                this.clearDocumentPreview();
            }
        });

    }

    attached() {
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

    loadDocumentToTest(selectedItem: string) {
        if (selectedItem) {
            var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
            loadDocTask.done(document => {
                this.beforePatch(JSON.stringify(document.toDto(), null, 4));
            }).fail(this.clearDocumentPreview());
        } else {
            this.clearDocumentPreview();
        }
    }

    private clearDocumentPreview() {
        this.beforePatch('');
        this.afterPatch('');
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
        list.fetch(0, 20);
        $("#matchingDocumentsGrid").resize();
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
        this.patchDocument().selectedItem(indexName);
        this.runQuery();
    }

    runQuery(): pagedList {
        var selectedIndex = this.patchDocument().selectedItem();
        if (selectedIndex) {
            var queryText = this.patchDocument().query();
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

    runQueryWithDelay() {
        setTimeout(() => this.runQuery(), 1000);
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
                this.afterPatch(JSON.stringify(testResult.toDto(), null, 4));
                this.updateActions(result[0].AdditionalData['Actions']);
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));
    }

    private updateActions(actions: { PutDocument: any[]; LoadDocument: any }) {
        this.loadedDocuments(actions.LoadDocument); 
        this.putDocuments(actions.PutDocument.map(doc => jsonUtil.syntaxHighlight(doc)));
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
                query = "Tag:" + this.patchDocument().selectedItem();
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
                this.afterPatch('');
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
}

export = patch;