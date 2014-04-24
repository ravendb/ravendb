import viewModelBase = require("viewmodels/viewModelBase");
import patchDocuments = require("models/patchDocuments");
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

class patch extends viewModelBase {

    displayName = "patch";
    indexNames = ko.observableArray<string>();
    collections = ko.observableArray<collection>();

    currentCollectionPagedItems = ko.observable<pagedList>();
    selectedDocumentIndices = ko.observableArray<number>();

    patchDocuments = ko.observable<patchDocuments>();

    beforePatch = ko.observable<string>();
    afterPatch = ko.observable<string>();

    constructor() {
        super();

        aceEditorBindingHandler.install();
    }

    activate() {
        var self = this;
        this.patchDocuments(patchDocuments.empty());
    }

    loadDocumentToPatch(selectedItem: string) {
        var loadDocTask = new getDocumentWithMetadataCommand(selectedItem, this.activeDatabase()).execute();
        loadDocTask.done(document => {
            this.beforePatch(JSON.stringify(document.toDto(), null, 4));
        }).fail(this.clearDocumentPreview());
    }

    private clearDocumentPreview() {
        this.beforePatch('');
        this.afterPatch('');
    }

    setSelectedPatchOnOption(patchOnOption: string) {
        this.patchDocuments().patchOnOption(patchOnOption);
        this.patchDocuments().selectedItem('');
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
        this.patchDocuments().selectedItem(coll.name);
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
        this.patchDocuments().selectedItem(indexName);
        this.runQuery();
    }

    runQuery(): pagedList {
        var selectedIndex = this.patchDocuments().selectedItem();
        if (selectedIndex) {
            var queryText = this.patchDocuments().query();
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
}

export = patch;