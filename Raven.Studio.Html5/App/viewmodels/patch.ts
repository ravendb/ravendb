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

class patch extends viewModelBase {

    displayName = "patch";
    indexNames = ko.observableArray<string>();
    collections = ko.observableArray<collection>();

    currentCollectionPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    patchDocument = ko.observable<patchDocument>();

    beforePatch = ko.observable<string>();
    afterPatch = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        var self = this;
        this.patchDocument(patchDocument.empty());
    }

    loadDocumentToPatch(selectedItem: string) {
        if (selectedItem) {
            var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
            loadDocTask.done(document => {
                this.beforePatch(JSON.stringify(document.toDto(), null, 4));
            }).fail(this.clearDocumentPreview());
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
}

export = patch;