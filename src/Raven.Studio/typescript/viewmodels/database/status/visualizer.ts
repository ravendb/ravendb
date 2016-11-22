import app = require("durandal/app");

import viewModelBase = require("viewmodels/viewModelBase");

import d3 = require('d3');

import visualizerGraphGlobal = require("viewmodels/database/status/visualizerGraphGlobal");

import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import getIndexMapReduceTreeCommand = require("commands/database/index/getIndexMapReduceTreeCommand");
import getIndexDebugSourceDocumentsCommand = require("commands/database/index/getIndexDebugSourceDocumentsCommand");

class visualizer extends viewModelBase {

    static readonly noIndexSelected = "Select an index";

    indexes = ko.observableArray<string>();
    indexName = ko.observable<string>();

    private currentIndex = ko.observable<string>();

    private currentIndexUi: KnockoutComputed<string>;
    private hasIndexSelected: KnockoutComputed<boolean>;

    private detailsViewActive = ko.observable<boolean>(false);

    private documents = {
        docKey: ko.observable(""),
        hasFocusDocKey: ko.observable<boolean>(false),
        loadingDocKeySearchResults: ko.observable<boolean>(false), //TODO: autocomplete support
        docKeys: ko.observableArray<string>(),
        docKeysSearchResults: ko.observableArray<string>()
    }

    private globalGraph = new visualizerGraphGlobal();

    constructor() {
        super();

        this.initObservables();
    }

    private initObservables() {
        this.currentIndexUi = ko.pureComputed(() => {
            const currentIndex = this.currentIndex();
            return currentIndex || visualizer.noIndexSelected;
        });

        this.hasIndexSelected = ko.pureComputed(() => !!this.currentIndex());

        this.documents.hasFocusDocKey.subscribe(value => {
            if (!value) {
                return;
            }
            this.fetchDocKeySearchResults("");
        });

        this.documents.docKey.throttle(100).subscribe(query => this.fetchDocKeySearchResults(query));
    }

    activate(args: any) {
        return new getIndexesStatsCommand(this.activeDatabase())
            .execute()
            .done(result => this.onIndexesLoaded(result));
    }

    compositionComplete() {
        super.compositionComplete();

        this.globalGraph.init((treeName: string) => this.goToDetails(treeName));
    }

    private onIndexesLoaded(indexes: Raven.Client.Data.Indexes.IndexStats[]) {
        this.indexes(indexes.filter(x => x.Type === "AutoMapReduce" || x.Type === "MapReduce").map(x => x.Name));
    }

    setSelectedIndex(indexName: string) {
        this.currentIndex(indexName);

        this.resetGraph();
    }

    private resetGraph() {
        this.documents.docKeys([]);
        this.documents.docKey("");
        this.documents.docKeysSearchResults([]);

        this.globalGraph.reset();
    }

    addDocKey(key: string) {
        if (!key) {
            return;
        }

        if (this.documents.docKeys.contains(key)) {
            this.globalGraph.zoomToDocument(key);
        } else {
            //TODO: spinner
            new getIndexMapReduceTreeCommand(this.activeDatabase(), this.currentIndex(), key)
                .execute()
                .done((mapReduceTrees) => {
                    if (!this.documents.docKeys.contains(key)) {
                        this.documents.docKeys.push(key);
                        this.globalGraph.addTrees(key, mapReduceTrees);
                        //TODO: pass to graph
                    }
                });
        }
    }

    goToDetails(treeName: string) {
        console.log("loading details for: " + treeName);
        this.detailsViewActive(true);
    }

    backToGlobalView() {
        this.detailsViewActive(false);

        this.globalGraph.restoreView();
    }

    selectDocKey(value: string) {
        this.addDocKey(value);
        this.documents.docKey("");
        this.documents.docKeysSearchResults.removeAll();
    }

    private fetchDocKeySearchResults(query: string) {
        this.documents.loadingDocKeySearchResults(true);

        new getIndexDebugSourceDocumentsCommand(this.activeDatabase(), this.currentIndex(), query, 0, 10)
            .execute()
            .done(result => {
                if (this.documents.docKey() === query) {
                    this.documents.docKeysSearchResults(result.Results);
                }
            })
            .always(() => this.documents.loadingDocKeySearchResults(false));
    }

    /*
        TODO @gregolsky apply google analytics
    */
}

export = visualizer;
