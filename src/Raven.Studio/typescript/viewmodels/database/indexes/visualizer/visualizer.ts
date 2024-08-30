import visualizerGraphGlobal = require("viewmodels/database/indexes/visualizer/visualizerGraphGlobal");
import visualizerGraphDetails = require("viewmodels/database/indexes/visualizer/visualizerGraphDetails");

import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import getIndexMapReduceTreeCommand = require("commands/database/index/getIndexMapReduceTreeCommand");
import getIndexDebugSourceDocumentsCommand = require("commands/database/index/getIndexDebugSourceDocumentsCommand");
import eventsCollector = require("common/eventsCollector");
import shardViewModelBase from "viewmodels/shardViewModelBase";
import database from "models/resources/database";
import { range } from "common/typeUtils";

type autoCompleteItem = {
    label: string;
    alreadyAdded: boolean;
}

class visualizer extends shardViewModelBase {

    view = require("views/database/indexes/visualizer/visualizer.html");

    static readonly noIndexSelected = "Select an index";

    indexes = ko.observableArray<string>();
    indexName = ko.observable<string>();
    
    private currentIndex = ko.observable<string>();

    private currentIndexUi: KnockoutComputed<string>;
    private hasIndexSelected: KnockoutComputed<boolean>;

    spinners = {
        addDocument: ko.observable<boolean>(false)
    };

    private documents = {
        documentId: ko.observable(""),
        hasFocusDocumentId: ko.observable<boolean>(false),
        documentIds: ko.observableArray<string>(),
        documentIdsSearchResults: ko.observableArray<autoCompleteItem>()
    };

    private trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[] = [];

    private globalGraph = new visualizerGraphGlobal();
    private detailsGraph = new visualizerGraphDetails();

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);
        
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

    activate(args: { index: string }) {
        return new getIndexesStatsCommand(this.db, this.location)
            .execute()
            .done(result => {
                this.onIndexesLoaded(result);
                if (args.index) {
                    this.currentIndex(args.index);
                }
            });
    }

    compositionComplete() {
        super.compositionComplete();
        
        this.globalGraph.init((treeName: string) => this.detailsGraph.openFor(treeName), doc => this.removeDocument(doc.name));
        this.detailsGraph.init(() => this.globalGraph.restoreView(), this.trees);
    }

    private onIndexesLoaded(indexes: Raven.Client.Documents.Indexes.IndexStats[]) {
        this.indexes(indexes.filter(x => x.Type === "AutoMapReduce" || x.Type === "MapReduce" || x.Type === "JavaScriptMapReduce").map(x => x.Name));
    }

    setSelectedIndex(indexName: string) {
        this.currentIndex(indexName);

        eventsCollector.default.reportEvent("visualizer", "set-index");
        
        this.resetGraph();
    }

    private resetGraph() {
        this.documents.documentIds([]);
        this.documents.documentId("");
        this.documents.documentIdsSearchResults([]);
        
        this.globalGraph.reset();
        this.detailsGraph.reset();
        this.trees.length = 0; // don't use = [], here are we have to retain reference
    }

    addCurrentDocumentId() {
        this.addDocumentId(this.documents.documentId());
        this.selectDocumentId("");
    }

    private addDocumentId(documentId: string) {
        if (!documentId) {
            return;
        }
        
        documentId = documentId.toLocaleLowerCase();

        const expandedDocumentIds = visualizer.maybeExpandDocumentIds(documentId);

        const documentsToFetch = expandedDocumentIds.filter(x => !_.includes(this.documents.documentIds(), x));

        if (documentsToFetch.length) {
            this.spinners.addDocument(true);

            new getIndexMapReduceTreeCommand(this.db, this.location, this.currentIndex(), documentsToFetch)
                .execute()
                .done((mapReduceTrees) => {
                    if (mapReduceTrees.length) {
                        documentsToFetch.forEach(docId => {
                            if (!_.includes(this.documents.documentIds(), docId)) {
                                this.documents.documentIds.push(docId);
                                this.addDocument(docId);
                            }
                        });

                        this.addTrees(mapReduceTrees);

                        this.globalGraph.zoomToDocument(documentsToFetch[0]);
                    }
                })
                .always(() => this.spinners.addDocument(false));
        } else {
            // we already have all documents - zoom to first one
            this.globalGraph.zoomToDocument(documentsToFetch[0]);
        }
    }

    /**
     * If document id contains -- then exand documents ids into list
     * Ex.: orders/1--3, expands to: orders/1, orders/2, orders/3
     */
    private static maybeExpandDocumentIds(input: string): Array<string> {
        if (input.includes("--")) {
            const tokens = input.split("--");
            if (tokens.length === 2) {
                const firstPart = tokens[0];
                const lastPart = tokens[1];
                const separatorIdx = firstPart.lastIndexOf("/");
                if (separatorIdx >= 0) {
                    const prefix = firstPart.substr(0, separatorIdx);
                    const rangeStart = firstPart.substr(separatorIdx + 1);
                    const rangeEnd = lastPart;

                    if (/\d+/.test(rangeStart) && /\d+/.test(rangeEnd)) {
                        const rangeStartInt = parseInt(rangeStart, 10);
                        const rangeEndInt = parseInt(rangeEnd, 10);
                        if (rangeStartInt <= rangeEndInt) {
                            return range(rangeStartInt, rangeEndInt, 1).map(x => prefix + "/" + x);
                        }
                    }
                }
            }
        }
        return [input];
    }

    private addDocument(docName: string) {       
        this.globalGraph.addDocument(docName);
        this.detailsGraph.addDocument(docName);
    }

    private removeDocument(documentName: string) {
        this.documents.documentIds.remove(documentName);
        this.globalGraph.removeDocument(documentName);
        this.detailsGraph.removeDocument(documentName);

        const toDelete: Raven.Server.Documents.Indexes.Debugging.ReduceTree[] = [];

        for (let i = 0; i < this.trees.length; i++) {
            const tree = this.trees[i];
            const leafs = visualizer.extractLeafs(tree.Root);

            let containsDifferentReference = false;

            leafs.forEach(leaf => {
                leaf.Entries.map(entry => {
                    if (entry.Source === documentName) {
                        entry.Source = null;
                    }
                });

                if (leaf.Entries.some(e => e.Source && e.Source !== documentName)) {
                    containsDifferentReference = true;
                }
            });

            if (!containsDifferentReference) {
                toDelete.push(tree);
            }
        }

        toDelete.forEach(x => _.pull(this.trees, x));

        this.globalGraph.syncTrees(this.trees);
    }

    private addTrees(result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        const treesToAdd: Raven.Server.Documents.Indexes.Debugging.ReduceTree[] = [];

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
        };

        visitor(root);

        return result;
    }

    selectDocumentId(value: string) {
        this.addDocumentId(value);
        this.documents.documentId("");
        this.documents.documentIdsSearchResults.removeAll();
    }

    private fetchDocumentIdSearchResults(query: string) {
        this.spinners.addDocument(true);

        const currentDocumentIds = this.documents.documentIds();

        new getIndexDebugSourceDocumentsCommand(this.db, this.location, this.currentIndex(), query, 0, 10)
            .execute()
            .done(result => {
                if (this.documents.documentId() === query) {
                    this.documents.documentIdsSearchResults(result.Results.map(x => ({
                        label: x,
                        alreadyAdded: _.includes(currentDocumentIds, x)
                    })));
                }
            })
            .always(() => this.spinners.addDocument(false));
    }
}

export = visualizer;
