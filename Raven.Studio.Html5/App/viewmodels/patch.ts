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
import queryIndexCommand = require("commands/queryIndexCommand");
import getDocumentWithMetadataCommand = require("commands/getDocumentWithMetadataCommand");
import savePatch = require('viewmodels/savePatch');
import loadPatch = require('viewmodels/loadPatch');
import savePatchCommand = require('commands/savePatchCommand');
import executeBulkDocsCommand = require("commands/executeBulkDocsCommand");
import virtualTable = require("widgets/virtualTable/viewModel");

class patch extends viewModelBase {

    displayName = "patch";
    indexNames = ko.observableArray<string>();
    collections = ko.observableArray<collection>();

    currentCollectionPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    patchDocument = ko.observable<patchDocument>();

    beforePatch = ko.observable<string>();
    afterPatch = ko.observable<string>();

    isExecuteAllowed: KnockoutComputed<boolean>;
    documentKey = ko.observable<string>();
    keyOfTestedDocument: KnockoutComputed<string>;

    static gridSelector = "#matchingDocumentsGrid";

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
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

    loadDocumentToPatch(selectedItem: string) {
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
        }
    }

    fetchAllCollections(): JQueryPromise<any> {
        return new getCollectionsCommand(this.activeDatabase())
            .execute()
            .done((colls: collection[]) => {
                this.collections(colls);
                if (this.collections().length > 0) {
                    this.setSelectedCollection(this.collections().first());
                }
            });
    }

    setSelectedCollection(coll: collection) {
        this.patchDocument().selectedItem(coll.name);
        this.currentCollectionPagedItems(coll.getDocuments());
    }

    fetchAllIndexes(): JQueryPromise<any> {
        return new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((results: databaseStatisticsDto) => {
                this.indexNames(results.Indexes.map(i => i.PublicName));
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

    savePatch() {
        var savePatchViewModel: savePatch = new savePatch();
        app.showDialog(savePatchViewModel);
        savePatchViewModel.onExit().done((patchName) => {
            new savePatchCommand(patchName, this.patchDocument(), this.activeDatabase()).execute();
        });
    }

    loadPatch() {
        var loadPatchViewModel: loadPatch = new loadPatch(this.activeDatabase());
        app.showDialog(loadPatchViewModel);
        loadPatchViewModel.onExit().done((patch) => {
            this.patchDocument(patch.cloneWithoutMetadata());
            this.loadDocumentToPatch(patch.selectedItem());
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
        new executeBulkDocsCommand(bulkDocs, this.activeDatabase())
            .execute()
            .done((result: bulkDocumentDto[]) => {
                var testResult = new document(result[0].AdditionalData['Document']);
                this.afterPatch(JSON.stringify(testResult.toDto(), null, 4));
            })
            .fail((result: JQueryXHR) => console.log(result.responseText));
    }

    executePatchOnSingle() {
        var keys = [];
        keys.push(this.patchDocument().selectedItem());
        this.executePatch(keys);
    }

    executePatchOnSelected() {
        this.executePatch(this.getDocumentsGrid().getSelectedItems().map(doc => doc.__metadata.id));
    }

    executePatchOnAll() {
        this.executePatch(this.currentCollectionPagedItems().getAllCachedItems().map(doc => doc.__metadata.id));
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
        new executeBulkDocsCommand(bulkDocs, this.activeDatabase())
            .execute()
            .done((result: bulkDocumentDto[]) => {
                this.afterPatch('');
                this.loadDocumentToPatch(this.patchDocument().selectedItem());
            })
            .fail((result: JQueryXHR) => console.log(result.responseText))
    }

    getDocumentsGrid(): virtualTable {
        var gridContents = $(patch.gridSelector).children()[0];
        if (gridContents) {
            return ko.dataFor(gridContents);
        }

        return null;
    }
}

export = patch;