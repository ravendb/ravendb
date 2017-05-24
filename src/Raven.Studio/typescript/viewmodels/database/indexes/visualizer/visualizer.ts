import viewModelBase = require("viewmodels/viewModelBase");
import visualizerGraphGlobal = require("viewmodels/database/indexes/visualizer/visualizerGraphGlobal");
import visualizerGraphDetails = require("viewmodels/database/indexes/visualizer/visualizerGraphDetails");

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

    private documents = {
        documentId: ko.observable(""),
        hasFocusDocumentId: ko.observable<boolean>(false),
        loadingDocumentIdSearchResults: ko.observable<boolean>(false), //TODO: autocomplete support
        documentIds: ko.observableArray<string>(),
        documentIdsSearchResults: ko.observableArray<string>()
    }

    private trees = [] as Raven.Server.Documents.Indexes.Debugging.ReduceTree[];

    private globalGraph = new visualizerGraphGlobal();
    private detailsGraph = new visualizerGraphDetails();

    constructor() {
        super();

        this.bindToCurrentInstance("setSelectedIndex", "selectDocumentId", "addCurrentDocumentId");

        this.initObservables();
    }

    private initObservables() {
        this.currentIndexUi = ko.pureComputed(() => {
            const currentIndex = this.currentIndex();
            return currentIndex || visualizer.noIndexSelected;
        });

        this.hasIndexSelected = ko.pureComputed(() => !!this.currentIndex());

        this.documents.hasFocusDocumentId.subscribe(value => {
            if (!value) {
                return;
            }
            this.fetchDocumentIdSearchResults("");
        });

        this.documents.documentId.throttle(100).subscribe(query => this.fetchDocumentIdSearchResults(query));
    }

    activate(args: any) {
        return new getIndexesStatsCommand(this.activeDatabase())
            .execute()
            .done(result => this.onIndexesLoaded(result));
    }

    compositionComplete() {
        super.compositionComplete();

        this.globalGraph.init((treeName: string) => this.detailsGraph.openFor(treeName));
        this.detailsGraph.init(() => this.globalGraph.restoreView(), this.trees);
    }

    private onIndexesLoaded(indexes: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.indexes(indexes.filter(x => x.Type === "AutoMapReduce" || x.Type === "MapReduce").map(x => x.Name));
    }

    setSelectedIndex(indexName: string) {
        this.currentIndex(indexName);

        this.resetGraph();
    }

    private resetGraph() {
        this.documents.documentIds([]);
        this.documents.documentId("");
        this.documents.documentIdsSearchResults([]);
        
        this.globalGraph.reset();
        this.detailsGraph.reset();
    }

    addCurrentDocumentId() {
        this.addDocumentId(this.documents.documentId());
    }

    private addDocumentId(documentId: string) {
        if (!documentId) {
            return;
        }

        if (_.includes(this.documents.documentIds(), documentId)) {
            this.globalGraph.zoomToDocument(documentId);
        } else {
            //TODO: spinner
            new getIndexMapReduceTreeCommand(this.activeDatabase(), this.currentIndex(), documentId)
                .execute()
                .done((mapReduceTrees) => {
                    if (!_.includes(this.documents.documentIds(), documentId)) {
                        this.documents.documentIds.push(documentId);

                        this.addDocument(documentId);
                        this.addTrees(mapReduceTrees);

                        this.globalGraph.zoomToDocument(documentId);
                    }
                });
        }
    }

    private addDocument(docName: string) {       
        this.globalGraph.addDocument(docName);
        this.detailsGraph.addDocument(docName);
    }

    private addTrees(result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        const treesToAdd = [] as Raven.Server.Documents.Indexes.Debugging.ReduceTree[];

        for (let i = 0; i < result.length; i++) {
            const incomingTree = result[i];

            const existingTree = this.trees.find(x => x.Name === incomingTree.Name);

            if (existingTree) {
                this.mergeTrees(incomingTree, existingTree);
                treesToAdd.push(existingTree);
            } else {
                treesToAdd.push(incomingTree);
                this.trees.push(incomingTree);
            }
        }

        this.globalGraph.addTrees(treesToAdd);
        this.detailsGraph.setDocumentsColors(this.globalGraph.getDocumentsColors());
    }

    private mergeTrees(incoming: Raven.Server.Documents.Indexes.Debugging.ReduceTree, mergeOnto: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        if (incoming.PageCount !== mergeOnto.PageCount || incoming.NumberOfEntries !== mergeOnto.NumberOfEntries) {
            throw new Error("Looks like tree data was changed. Can't render graph");
        }

        const existingLeafs = visualizer.extractLeafs(mergeOnto.Root);
        const newLeafs = visualizer.extractLeafs(incoming.Root);

        existingLeafs.forEach((page, pageNumber) => {
            const newPage = newLeafs.get(pageNumber);

            for (let i = 0; i < newPage.Entries.length; i++) {
                if (newPage.Entries[i].Source) {
                    page.Entries[i].Source = newPage.Entries[i].Source;
                }
            }
        });
    }

    private static extractLeafs(root: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage): Map<number, Raven.Server.Documents.Indexes.Debugging.ReduceTreePage> {
        const result = new Map<number, Raven.Server.Documents.Indexes.Debugging.ReduceTreePage>();

        const visitor = (node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) => {

            if (node.Entries && node.Entries.length) {
                result.set(node.PageNumber, node);
            }

            if (node.Children) {
                for (let i = 0; i < node.Children.length; i++) {
                    visitor(node.Children[i]);
                }
            }
        }

        visitor(root);

        return result;
    }

    selectDocumentId(value: string) {
        this.addDocumentId(value);
        this.documents.documentId("");
        this.documents.documentIdsSearchResults.removeAll();
    }

    private fetchDocumentIdSearchResults(query: string) {
        this.documents.loadingDocumentIdSearchResults(true);

        new getIndexDebugSourceDocumentsCommand(this.activeDatabase(), this.currentIndex(), query, 0, 10)
            .execute()
            .done(result => {
                if (this.documents.documentId() === query) {
                    this.documents.documentIdsSearchResults(result.Results);
                }
            })
            .always(() => this.documents.loadingDocumentIdSearchResults(false));
    }

    /*
        TODO @gregolsky apply google analytics
    */
}

export = visualizer;
