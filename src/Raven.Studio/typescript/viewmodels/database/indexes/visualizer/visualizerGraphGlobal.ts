import graphHelper = require("common/helpers/graph/graphHelper");
import viewHelpers = require("common/helpers/view/viewHelpers");
import canvasIcons = require("common/helpers/graph/canvasIcons");

import d3 = require('d3');
import rbush = require("rbush");

abstract class abstractPageItem {
    static readonly pageWidth = 20;
    static readonly pageHeight = 20;

    x: number;
    y: number;
    parent: reduceTreeItem;
    incomingLinesCount: number;

    aggregation?: pageItem;

    constructor(parent: reduceTreeItem) {
        this.parent = parent;
        this.incomingLinesCount = 0;
    }

    getSourceConnectionPoint(): [number, number] {
        return [this.x + pageItem.pageWidth / 2, this.y];
    }

    getTargetConnectionPoint(): [number, number] {
        return [this.x + pageItem.pageWidth / 2, this.y + pageItem.pageHeight];
    }

    getGlobalTargetConnectionPoint(): [number, number] {
        const localConnectionPoint = this.getTargetConnectionPoint();
        return [localConnectionPoint[0] + this.parent.x, localConnectionPoint[1] + this.parent.y];
    }

    // Return the X offset from where to start drawing the first incoming line to the pageItem
    getGlobalTargetConnectionPointXOffset(): number {
        if (this.incomingLinesCount === 1)
            return 0;

        const segmentSize = 4;
        let xOffset = Math.floor(this.incomingLinesCount / 2) * segmentSize;
        if (this.incomingLinesCount % 2 === 0) {
            xOffset -= 2;
        }
        return xOffset;
    }
}

class pageItem extends abstractPageItem {
    
    pageNumber: number;

    constructor(pageNumber: number, parent: reduceTreeItem) {
        super(parent);
        this.pageNumber = pageNumber;
    }
    
}

class collapsedPageItem extends pageItem {

    aggregationCount: number = 1;
}

class reduceTreeItem {

    /*
    Top-bottom: treeMargin, titleHeight, treeMargin, pageHeight, betweenPagesVerticalPadding, ..., pageHeight, treeMargin
    Left-right: treeMargin, <dynamic text margin>, treeMargin, pageWidth, betweenPagesHorizontalPadding, ..., pageWidth, treeMargin
    */
    static readonly margins = {
        treeMargin: 20,
        betweenPagesHorizontalPadding: 13,
        betweenPagesVerticalPadding: 50,
        titleHeight: 20
    }

    private tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree;
    name: string;
    displayName: string;
    depth: number;
    itemsCountAtDepth: Array<number>; // this represents non-filtered count
    itemsAtDepth = new Map<number, Array<abstractPageItem>>(); // items after filtering

    width = 0;
    height = 0;
    totalLeafsNumberWidth = 0;
    x: number;
    y: number;
    extraLeftPadding = 0; //used when text (reduce key) is longer than contents

    constructor(tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        this.tree = tree;
        this.name = tree.Name;
        this.displayName = tree.DisplayName;
        this.depth = tree.Depth;

        this.countItemsPerDepth();
    }

    private countItemsPerDepth() {
        this.itemsCountAtDepth = new Array(this.depth);

        const countEntries = (depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) => {
            this.itemsCountAtDepth[depth] = (this.itemsCountAtDepth[depth] || 0) + 1;

            if (node.Children && node.Children.length) {
                for (let i = 0; i < node.Children.length; i++) {
                    countEntries(depth + 1, node.Children[i]);
                }
            }
        };

        countEntries(0, this.tree.Root);
    }

    filterAndLayoutVisibleItems(documents: documentItem[]) {
        this.cleanCache(documents);
        this.filterVisibleItems(documents);
        this.layout();
    }

    private cleanCache(documents: documentItem[]) {
        this.itemsAtDepth.clear();
    }

    private filterVisibleItems(documents: documentItem[]) {
        const filterAtDepth = (depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage, aggregation: pageItem) => {
            if (!this.itemsAtDepth.has(depth)) {
                this.itemsAtDepth.set(depth, []);
            }

            const items = this.itemsAtDepth.get(depth);
            const item = new pageItem(node.PageNumber, this);
            item.aggregation = aggregation;
            items.push(item);

            if (node.Children) {
                for (let i = 0; i < node.Children.length; i++) {
                    filterAtDepth(depth + 1, node.Children[i], item);
                }
            }

            if (node.Entries) {
                const uniqueSources = new Set<string>(node.Entries.filter(x => x.Source).map(x => x.Source));
                uniqueSources.forEach(source => {
                    const matchingDoc = documents.find(d => d.name === source);
                    item.incomingLinesCount++;
                    matchingDoc.connectedPages.push(item);
                });
            }
        };

        filterAtDepth(0, this.tree.Root, null);

        this.collapseNonRelevantLeafs();
    }

    private collapseNonRelevantLeafs() {
        const lastLevel = this.depth - 1;
        const levelItems = this.itemsAtDepth.get(lastLevel);

        const relevantPageNumbers = this.itemsAtDepth
            .get(this.depth - 1)
            .filter(x => x.incomingLinesCount > 0)
            .map((x: pageItem) => x.pageNumber);


        const collapsedItems = [] as Array<abstractPageItem>;
        let currentAggregation: collapsedPageItem = null;

        for (let i = 0; i < levelItems.length; i++) {
            const item = levelItems[i] as pageItem;
            if (_.includes(relevantPageNumbers, item.pageNumber)) {
                currentAggregation = null;
                collapsedItems.push(item);
            } else {
                if (currentAggregation && currentAggregation.aggregation === item.aggregation) {
                    currentAggregation.aggregationCount += 1;
                } else {
                    currentAggregation = new collapsedPageItem(item.aggregation.pageNumber, item.parent);
                    currentAggregation.aggregation = item.aggregation;
                    collapsedItems.push(currentAggregation);
                }
            }
        }

        this.itemsAtDepth.set(lastLevel, collapsedItems);
    }

    private getMaxVisibleItems() {
        let max = 0;
        this.itemsAtDepth.forEach(x => {
            if (x.length > max) {
                max = x.length;
            }
        });
        return max;
    }

    private layout() {
        this.totalLeafsNumberWidth = visualizerGraphGlobal.totalEntriesWidth(_.max(this.itemsCountAtDepth));
        this.calculateTreeDimensions();

        const maxItems = this.getMaxVisibleItems();
        const pagesTotalWidth = this.getPagesOnlyWidth(maxItems);

        const yOffset = pageItem.pageHeight +
            reduceTreeItem.margins.betweenPagesVerticalPadding;

        let yStart = reduceTreeItem.margins.treeMargin +
            reduceTreeItem.margins.titleHeight +
            reduceTreeItem.margins.treeMargin
            + (this.depth - 1) * yOffset;

        const avgX = new Map<number, { count: number, total: number }>();

        for (let depth = this.depth - 1; depth >= 0; depth--) {
            const items = this.itemsAtDepth.get(depth);

            if (depth === this.depth - 1) { // leafs level
                const startAndOffset = graphHelper
                    .computeStartAndOffset(pagesTotalWidth,
                    items.length,
                    pageItem.pageWidth);

                const xOffset = startAndOffset.offset;

                let xStart = reduceTreeItem.margins.treeMargin +
                    this.totalLeafsNumberWidth +
                    reduceTreeItem.margins.treeMargin +
                    this.extraLeftPadding +
                    startAndOffset.start;

                for (let i = 0; i < items.length; i++) {
                    const item = items[i];
                    item.x = xStart;
                    xStart += xOffset;
                }
            } else {
                for (let i = 0; i < items.length; i++) {
                    const item = items[i] as pageItem;
                    const avgItem = avgX.get(item.pageNumber);
                    item.x = avgItem.total / avgItem.count;
                }
            }

            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                item.y = yStart;

                if (item.aggregation) {
                    const avgItem = avgX.get(item.aggregation.pageNumber);
                    if (avgItem) {
                        avgItem.count++;
                        avgItem.total += item.x;
                    } else {
                        avgX.set(item.aggregation.pageNumber, { count: 1, total: item.x });
                    }
                }
            }

            yStart -= yOffset;
        }
    }

    private calculateTreeDimensions() {
        let height = reduceTreeItem.margins.treeMargin +
            reduceTreeItem.margins.titleHeight +
            reduceTreeItem.margins.treeMargin;

        height += (this.depth * pageItem.pageHeight) +
            (this.depth - 1) * reduceTreeItem.margins.betweenPagesVerticalPadding;

        height += reduceTreeItem.margins.treeMargin;

        this.height = height;

        const maxItems = this.getMaxVisibleItems();

        let width = reduceTreeItem.margins.treeMargin +
            this.totalLeafsNumberWidth +
            reduceTreeItem.margins.treeMargin;

        width += this.getPagesOnlyWidth(maxItems);

        width += reduceTreeItem.margins.treeMargin;

        const estimatedTextWidth = this.estimateReduceKeyWidth() + 2 * reduceTreeItem.margins.treeMargin;

        if (estimatedTextWidth > width) {
            this.extraLeftPadding = (estimatedTextWidth - width) / 2;
            width = estimatedTextWidth;
        } else {
            this.extraLeftPadding = 0;
        }

        this.width = width;
    }

    private estimateReduceKeyWidth() {
        return this.displayName.length * 6;
    }

    private getPagesOnlyWidth(maxItems: number) {
        return maxItems * pageItem.pageWidth +
            (maxItems + 1) * reduceTreeItem.margins.betweenPagesHorizontalPadding;
    }
}

class documentItem {
    static readonly margins = {
        minMarginBetweenDocumentNames: 30,
        badgePadding: 10,
        deleteAreaWidth: 20
    }

    name: string;

    x: number;
    y: number;
    width: number;
    height: number;
    color: string;

    connectedPages = [] as Array<pageItem>;

    constructor(name: string) {
        this.name = name;
    }

    getSourceConnectionPoint(): [number, number] {
        return [this.x + this.width / 2, this.y];
    }
}

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "reduceTreeClicked" | "trashClicked";
    arg: reduceTreeItem | documentItem;
}

class hitTest {

    cursor = ko.observable<string>("auto");
    private mouseDown = false;
    private currentPage: reduceTreeItem = null;

    private rTree = rbush<rTreeLeaf>();

    private onReduceTreeClicked: (item: reduceTreeItem) => void;
    private onTrashClicked: (item: documentItem) => void;
    private onReduceTreeEnter: (item: reduceTreeItem) => void;
    private onReduceTreeExit: (item: reduceTreeItem) => void;

    init(onReduceTreeClicked: (item: reduceTreeItem) => void, onTrashClicked: (item: documentItem) => void, onReduceTreeEnter: (item: reduceTreeItem) => void, onReduceTreeExit: (item: reduceTreeItem) => void) {
        this.onReduceTreeClicked = onReduceTreeClicked;
        this.onTrashClicked = onTrashClicked;
        this.onReduceTreeEnter = onReduceTreeEnter;
        this.onReduceTreeExit = onReduceTreeExit;
    }

    registerReduceTree(tree: reduceTreeItem) {
        this.rTree.insert({
            minX: tree.x,
            maxX: tree.x + tree.width,
            minY: tree.y,
            maxY: tree.y + tree.height,
            actionType: "reduceTreeClicked",
            arg: tree
        } as rTreeLeaf);
    }

    registerTrash(docItem: documentItem) {
        const trashWidth = documentItem.margins.deleteAreaWidth;
        this.rTree.insert({
            minX: docItem.x + docItem.width - trashWidth,
            maxX: docItem.x + docItem.width,
            minY: docItem.y,
            maxY: docItem.y + docItem.height,
            actionType: "trashClicked",
            arg: docItem
        } as rTreeLeaf);
    }

    reset() {
        this.rTree.clear();
    }

    onMouseMove(location: [number, number]) {
        const items = this.findItems(location[0], location[1]);

        const tree = items.find(x => x.actionType === "reduceTreeClicked");

        if (tree) {
            this.cursor(this.mouseDown ? graphHelper.prefixStyle("grabbing") : "pointer");

            const reduceTree = tree.arg as reduceTreeItem;
            if (this.currentPage !== reduceTree) {
                this.onReduceTreeEnter(reduceTree);
                this.currentPage = reduceTree;
            }
            
        } else {
            if (this.currentPage) {
                this.onReduceTreeExit(this.currentPage);
                this.currentPage = null;
            }

            if (items.filter(x => x.actionType === "trashClicked").length > 0) {
                this.cursor(this.mouseDown ? graphHelper.prefixStyle("grabbing") : "pointer");
            } else {
                this.cursor(graphHelper.prefixStyle(this.mouseDown ? "grabbing" : "grab"));
            }
        }
    }

    onClick(location: [number, number]) {
        const items = this.findItems(location[0], location[1]);

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            switch (item.actionType) {
                case "reduceTreeClicked":
                    this.onReduceTreeClicked(item.arg as reduceTreeItem);
                    break;
                case "trashClicked":
                    this.onTrashClicked(item.arg as documentItem);
                    break;
            }
            
        }
    }

    onMouseDown() {
        this.cursor(graphHelper.prefixStyle("grabbing"));
        this.mouseDown = true;
    }

    onMouseUp() {
        this.cursor(graphHelper.prefixStyle("grab"));
        this.mouseDown = false;
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y,
            maxY: y
        });
    }
}

interface avgXVals {
    docIndex: number;
    avgXVal: number;
}

class visualizerGraphGlobal {

    static readonly documentColors = ["#2196f5", "#ef6c5a", "#80ced0", "#9ccd64", "#f06292", "#7f45e6", "#fea724", "#01acc0"];
    private nextColorIndex = 0;

    static margins = {
        betweenPagesWidth: 30,
        betweenPagesAndDocuments: 80,
        globalMargin: 30
    };

    private hitTest = new hitTest();

    private savedZoomStatus = {
        scale: 1,
        translate: [0, 0] as [number, number]
    };

    private totalWidth: number;
    private totalHeight: number; 

    private documents = [] as Array<documentItem>;
    private reduceTrees: Array<reduceTreeItem> = [];

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;
    private currentReduceTreeHighlight: d3.Selection<void>;
    private reduceTreeHighlightHandler: () => void = () => { };

    private dataWidth = 0; // total width of all virtual elements
    private dataHeight = 0; // total heigth of all virtual elements

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    private goToDetailsCallback: (treeName: string) => void;
    private animationInProgress = false;
    private deleteItemCallback: (item: documentItem) => void;

    addDocument(documentName: string) {
        const document = new documentItem(documentName);
        this.documents.push(document);
    }

    removeDocument(documentName: string) {
        const matchedDocument = this.documents.find(x => x.name === documentName);
        if (matchedDocument) {
            _.pull(this.documents, matchedDocument);
        }
    }

    addTrees(result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        for (let i = 0; i < result.length; i++) {
            const reduceTree = result[i];
            const existingTree = this.reduceTrees.find(x => x.name === reduceTree.Name);
            if (!existingTree) {
                const newTree = new reduceTreeItem(reduceTree);
                this.reduceTrees.push(newTree);
            }
        }

        this.layout();
    }

    syncTrees(items: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        this.reduceTrees = [];
        for (let i = 0; i < items.length; i++) {
            const reduceTree = items[i];
            const newTree = new reduceTreeItem(reduceTree);
            this.reduceTrees.push(newTree);
        }

        this.layout();
        this.draw();
    }

    zoomToDocument(documentName: string) {
        const documentNameLowerCase = documentName.toLowerCase();
        const doc = this.documents.find(x => x.name.toLowerCase() === documentNameLowerCase);
        if (doc) {
            const requestedTranslation: [number, number] = [-doc.x + this.totalWidth / 2 - doc.width / 2, 0];
            if (this.documents.length === 1) {
                // we don't want animation on first element
                this.zoom.translate(requestedTranslation).scale(1).event(this.canvas);

            } else {
                this.canvas
                    .transition()
                    .duration(500)
                    .call(this.zoom.translate(requestedTranslation).scale(1).event);
            }
        }
    }

    reset() {
        this.documents = [];
        this.reduceTrees = [];
        this.hitTest.reset();

        this.draw();
    }

    init(goToDetailsCallback: (treeName: string) => void, deleteItemCallback: (item: documentItem) => void) {
        this.goToDetailsCallback = goToDetailsCallback;
        this.deleteItemCallback = deleteItemCallback;
        const container = d3.select("#visualizerContainer");

        [this.totalWidth, this.totalHeight] = viewHelpers.getPageHostDimenensions();

        this.canvas = container
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = container
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.xScale = d3.scale.linear()
            .domain([0, this.totalWidth])
            .range([0, this.totalWidth]);

        this.yScale = d3.scale.linear()
            .domain([0, this.totalHeight])
            .range([0, this.totalHeight]);

        this.zoom = d3.behavior.zoom<void>()
            .x(this.xScale)
            .y(this.yScale)
            .on("zoom", () => this.onZoom());

        this.currentReduceTreeHighlight = this.svg
            .append("svg:rect")
            .attr("class", "highlight")
            .attr("fill", "white")
            .style("opacity", 0);

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight)
            .call(this.zoom)
            .call(d => this.setupEvents(d));

        this.hitTest.init(item => this.onReduceTreeClicked(item), item => this.onTrashClicked(item), item => this.onReduceTreeEnter(item), item => this.onReduceTreeExit(item));
    }

    getDocumentsColors(): Array<documentColorPair> {
        const documentsColorsSetup: Array<documentColorPair> = [];

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            documentsColorsSetup.push( { docName: doc.name, docColor: doc.color} );
        }
        return documentsColorsSetup;
    }

    private setupEvents(selection: d3.Selection<void>) {
        selection
            .on("dblclick.zoom", null)
            .on("click", () => this.onClick())
            .on("mousemove", () => this.hitTest.onMouseMove(this.getMouseLocation()))
            .on("mouseup", () => this.hitTest.onMouseUp())
            .on("mousedown", () => this.hitTest.onMouseDown());

        this.hitTest.cursor.subscribe(cursor => {
            selection.style("cursor", cursor);
        });
    }

    private onZoom() {
        this.draw();
    }

    private onTrashClicked(item: documentItem) {
        viewHelpers.confirmationMessage("Are you sure?", "Do you want to remove document: " + item.name + " from analysis?", {
            buttons: ["No", "Yes, delete"]
        })
            .done(result => {
                if (result.can) {
                    this.deleteItemCallback(item);
                }
            });
    }

    private onReduceTreeClicked(item: reduceTreeItem) {
        this.animationInProgress = true;
        this.saveCurrentZoom();

        const requestedScale = Math.min(this.totalWidth / item.width, this.totalHeight / item.height);

        const extraXOffset = (this.totalWidth - requestedScale * item.width) / 2;

        const requestedTranslation: [number, number] = [-item.x * requestedScale + extraXOffset, -item.y * requestedScale];

        this.reduceTreeHighlightHandler = () => { };

        // cancel transition
        this.currentReduceTreeHighlight
            .transition()
            .duration(0)
            .style("opacity", 0);

        this.canvas
            .transition()
            .duration(500)
            .call(this.zoom.translate(requestedTranslation).scale(requestedScale).event)
            .style('opacity', 0)
            .each("end", () => {
                this.toggleUiElements(false);
                this.goToDetailsCallback(item.name);
                this.animationInProgress = false;
            });
    }

    private onReduceTreeEnter(item: reduceTreeItem) {
        if (this.animationInProgress) {
            return;
        }

        this.reduceTreeHighlightHandler = () => {
            const [x1, y1] = [this.xScale(item.x), this.yScale(item.y)];
            const [x2, y2] = [this.xScale(item.x + item.width), this.yScale(item.y + item.height)];

            this.currentReduceTreeHighlight
                .attr("width", x2 - x1)
                .attr("height", y2 - y1)
                .attr("x", x1)
                .attr("y", y1)
                .transition()
                .duration(200)
                .style("opacity", 0.1);
        }

        this.reduceTreeHighlightHandler();
    }

    private onReduceTreeExit(item: reduceTreeItem) {
        if (this.animationInProgress) {
            return;
        }

        this.reduceTreeHighlightHandler = () => {
            this.currentReduceTreeHighlight
                .transition()
                .duration(200)
                .style("opacity", 0);
        }

        this.reduceTreeHighlightHandler();
    }

    restoreView() {
        this.animationInProgress = true;
        this.toggleUiElements(true);

        this.canvas
            .transition()
            .duration(500)
            .call(this.zoom.translate(this.savedZoomStatus.translate).scale(this.savedZoomStatus.scale).event)
            .style('opacity', 1)
            .each("end", () => this.animationInProgress = false);
    }

    private saveCurrentZoom() {
        this.savedZoomStatus.scale = this.zoom.scale();
        this.savedZoomStatus.translate = this.zoom.translate();
    }


    private onClick() {
        if ((d3.event as any).defaultPrevented) {
            return;
        }

        this.hitTest.onClick(this.getMouseLocation());
    }

    private getMouseLocation(): [number, number] {
        const clickLocation = d3.mouse(this.canvas.node());
        return [this.xScale.invert(clickLocation[0]), this.yScale.invert(clickLocation[1])];
    }

    private cleanLayoutCache() {
        for (let i = 0; i < this.documents.length; i++) {
            this.documents[i].connectedPages = [];
        }
    }

    private layout() {
        this.cleanLayoutCache();

        if (!this.reduceTrees.length) {
            this.dataWidth = 0;
            this.dataHeight = 0;
            this.hitTest.reset();
            return;
        }

        // layout children first
        for (let i = 0; i < this.reduceTrees.length; i++) {
            const tree = this.reduceTrees[i];
            tree.filterAndLayoutVisibleItems(this.documents);
        }

        const maxTreeHeight = d3.max(this.reduceTrees, x => x.height);

        let currentX = visualizerGraphGlobal.margins.globalMargin;
        for (let i = 0; i < this.reduceTrees.length; i++) {
            const tree = this.reduceTrees[i];

            tree.x = currentX;
            tree.y = visualizerGraphGlobal.margins.globalMargin + (maxTreeHeight - tree.height);

            currentX += tree.width;
            currentX += visualizerGraphGlobal.margins.betweenPagesWidth;
        }

        currentX -= visualizerGraphGlobal.margins.betweenPagesWidth;
        currentX += visualizerGraphGlobal.margins.globalMargin;

        this.dataWidth = currentX;

        const documentNamesYStart =
            visualizerGraphGlobal.margins.globalMargin +
                maxTreeHeight +
                visualizerGraphGlobal.margins.betweenPagesAndDocuments;

        const height = documentNamesYStart
            + 100 
            + visualizerGraphGlobal.margins.globalMargin; // top and bottom margin
        this.dataHeight = height;

        this.layoutDocuments(documentNamesYStart);
        this.registerHitAreas();
    }

    private layoutDocuments(yStart: number) {
        let totalWidth = 0;

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            const documentNameWidthEstimation = (text: string) => text.length * 10;


            doc.width = documentItem.margins.badgePadding * 2 + documentNameWidthEstimation(doc.name) + documentItem.margins.deleteAreaWidth;
            doc.height = 35;
            doc.y = yStart;

            totalWidth += doc.width;
        }

        totalWidth += this.documents.length * (documentItem.margins.minMarginBetweenDocumentNames + 1);

        if (totalWidth > this.dataWidth) {
            //TODO: handle me!
        }

        for (let currentDoc = 0; currentDoc < this.documents.length; currentDoc++) {
            const doc = this.documents[currentDoc];

            // Calc document x value as average of connectedPages
            const totalXValuesOfConnectedPages = doc.connectedPages.reduce((a, b) => a + b.x + b.parent.x + pageItem.pageWidth / 2, 0);
            doc.x = totalXValuesOfConnectedPages / doc.connectedPages.length;
        }

        this.nextColorIndex = 0;
        this.documents.forEach(x => x.color = this.getNextColor());

        const xPadding = 20;

        graphHelper.layoutUsingNearestCenters(this.documents, xPadding);
    }

    private registerHitAreas() {
        this.hitTest.reset();

        for (let i = 0; i < this.reduceTrees.length; i++) {
            this.hitTest.registerReduceTree(this.reduceTrees[i]);
        }

        for (let i = 0; i < this.documents.length; i++) {
            this.hitTest.registerTrash(this.documents[i]);
        }
    }

    private draw() {
        const canvas = this.canvas.node() as HTMLCanvasElement;
        const ctx = canvas.getContext("2d");

        ctx.clearRect(0, 0, this.totalWidth, this.totalHeight);
        ctx.save();

        const translation = this.zoom.translate();
        ctx.translate(translation[0], translation[1]);
        ctx.scale(this.zoom.scale(), this.zoom.scale());

        for (let i = 0; i < this.reduceTrees.length; i++) {
            this.drawTree(ctx, this.reduceTrees[i]);
        }

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            this.drawDocument(ctx, doc);
        }

        this.drawDocumentConnections(ctx);
        ctx.restore();
        this.reduceTreeHighlightHandler();
    }

    private drawTree(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {
        ctx.fillStyle = "#2c3333";
        ctx.fillRect(tree.x, tree.y, tree.width, tree.height);

        ctx.save();
        try {
            ctx.translate(tree.x, tree.y);

            // text
            ctx.beginPath();
            ctx.font = "10px Lato";
            ctx.textAlign = "center";
            ctx.fillStyle = "#f0f4f6";
            ctx.strokeStyle = "#686f6f";
            ctx.fillText(tree.displayName, tree.width / 2, reduceTreeItem.margins.treeMargin, tree.width);

            // total entries
            let totalEntiresY = reduceTreeItem.margins.treeMargin +
                reduceTreeItem.margins.titleHeight +
                reduceTreeItem.margins.treeMargin;
            const totalEntriesOffset = pageItem.pageHeight +
                reduceTreeItem.margins.betweenPagesVerticalPadding;

            ctx.fillStyle = "#686f6f";
            for (let i = 1; i < tree.depth; i++) {
                totalEntiresY += totalEntriesOffset;
                const items = tree.itemsCountAtDepth[i];
                ctx.beginPath();
                ctx.font = "18px Lato";
                ctx.textAlign = "right";
                ctx.fillText(items.toString(),
                    reduceTreeItem.margins.treeMargin + tree.totalLeafsNumberWidth,
                    totalEntiresY + 16);

                const spliterX = reduceTreeItem.margins.treeMargin + tree.totalLeafsNumberWidth + 4;

                ctx.beginPath();
                ctx.moveTo(spliterX, totalEntiresY - 6);
                ctx.lineTo(spliterX, totalEntiresY + 26);
                ctx.stroke();
            }

            this.drawPages(ctx, tree);
        } finally {
            ctx.restore();
        }
    }

    private drawPages(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {

        ctx.fillStyle = "#008cc9";
        ctx.strokeStyle = "#686f6f";

        tree.itemsAtDepth.forEach(globalItems => {
            for (let i = 0; i < globalItems.length; i++) {
                const item = globalItems[i];

                if (item instanceof collapsedPageItem) {
                    ctx.save();
                    ctx.translate(item.x, item.y);
                    ctx.beginPath();

                    ctx.rect(0, 0, 6, 2);
                    ctx.rect(0, 0, 2, 6);

                    ctx.rect(14, 0, 6, 2);
                    ctx.rect(18, 0, 2, 6);

                    ctx.rect(0, 14, 2, 6);
                    ctx.rect(0, 18, 6, 2);

                    ctx.rect(14, 18, 6, 2);
                    ctx.rect(18, 14, 2, 6);

                    ctx.rect(4, 9, 3, 3);
                    ctx.rect(8, 9, 3, 3);
                    ctx.rect(12, 9, 3, 3);

                    ctx.fill();
                    ctx.restore();
                    
                } else {
                    ctx.fillRect(item.x, item.y, pageItem.pageWidth, pageItem.pageHeight);
                }

                if (item.aggregation) {
                    const sourcePoint = item.getSourceConnectionPoint();
                    const targetPoint = item.aggregation.getTargetConnectionPoint();
                    graphHelper.drawBezierDiagonal(ctx, sourcePoint, targetPoint, true);
                }
            }
        });
    }

    private drawDocumentConnections(ctx: CanvasRenderingContext2D) {
        ctx.lineWidth = 2;
        let straightLine = 8;
        const targetXValuesDrawnOnScreen: number[] = [];

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            ctx.strokeStyle = doc.color;
            straightLine += 5;

            for (let j = 0; j < doc.connectedPages.length; j++) {
                const page = doc.connectedPages[j];

                const targetXY: [number, number] = page.getGlobalTargetConnectionPoint();
                let targetXOffset = targetXY[0] - page.getGlobalTargetConnectionPointXOffset();

                //TODO  it is hot path
                while (_.includes(targetXValuesDrawnOnScreen, targetXOffset)) {
                    targetXOffset += 4;
                }
                targetXY[0] = targetXOffset;

                const sourceXY: [number, number] = doc.getSourceConnectionPoint();

                if (straightLine > 60) {
                    straightLine = 11;
                }

                this.drawStraightLines(ctx, sourceXY, targetXY, straightLine);

                // Save x value of target to avoid collisions
                targetXValuesDrawnOnScreen.push(targetXY[0]);
            }
        }
    }

    private drawStraightLines(ctx: CanvasRenderingContext2D, source: [number, number], target: [number, number], straightLine: number) {
        ctx.beginPath();

        if (source[1] < target[1]) {
            ctx.moveTo(source[0], source[1]);
            ctx.lineTo(source[0], source[1] + straightLine);
            ctx.lineTo(target[0], source[1] + straightLine);
            ctx.lineTo(target[0], target[1]);
            ctx.stroke();
        } else {
            ctx.moveTo(source[0], source[1]);
            ctx.lineTo(source[0], source[1] - straightLine);
            ctx.lineTo(target[0], source[1] - straightLine);
            ctx.lineTo(target[0], target[1]);
            ctx.stroke();
        }

        // Draw arrow
        const halfWidth = 6;
        const height = 8;
        ctx.beginPath();
        ctx.moveTo(target[0] - halfWidth, target[1] + height);
        ctx.lineTo(target[0], target[1]);
        ctx.lineTo(target[0] + halfWidth, target[1] + height);
        ctx.stroke();
    }

    private drawDocument(ctx: CanvasRenderingContext2D, docItem: documentItem) {
        ctx.fillStyle = docItem.color;
        ctx.fillRect(docItem.x, docItem.y, docItem.width, docItem.height);
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.font = "18px Lato";
        ctx.fillStyle = "black";
        ctx.fillText(docItem.name, docItem.x + (docItem.width - documentItem.margins.deleteAreaWidth) / 2 , docItem.y + docItem.height / 2);
        
        const offsetX = -5;
        const offsetY = 11;
        canvasIcons.cancel(ctx, docItem.x + docItem.width - documentItem.margins.deleteAreaWidth + offsetX, docItem.y + offsetY, 16);
    }

    private toggleUiElements(show: boolean) {
        this.svg.style("display", show ? "block" : "none");
        this.canvas.style("display", show ? "block" : "none");
    }

    static totalEntriesWidth(entries: number) {
        return Math.ceil(Math.log10(entries)) * 13;
    }

    private getNextColor() {
        const color = visualizerGraphGlobal.documentColors[this.nextColorIndex % visualizerGraphGlobal.documentColors.length];
        this.nextColorIndex++;
        return color;
    }
}

export = visualizerGraphGlobal;
