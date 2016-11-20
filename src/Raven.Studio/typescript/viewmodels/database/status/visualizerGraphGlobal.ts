import app = require("durandal/app");
import graphHelper = require("common/helpers/graph/graphHelper");

import d3 = require('d3');

class pageItem {
    static readonly pageWidth = 20;
    static readonly pageHeight = 20;
    
    x: number;
    y: number;
    aggregation?: pageItem;
    pageNumber: number;
    parent: reduceTreeItem;

    constructor(pageNumber: number, parent: reduceTreeItem) {
        this.pageNumber = pageNumber;
        this.parent = parent;
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
    depth: number;
    itemsCountAtDepth: Array<number>; // this represents non-filtered count
    itemsAsDepth = new Map<number, Array<pageItem>>(); // items after filtering

    width = 0;
    height = 0;
    x: number;
    y: number;
    extraLeftPadding = 0; //used when text (reduce key) is longer than contents

    constructor(tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        this.tree = tree;
        this.name = tree.Name;
        this.depth = tree.Depth;

        this.countItemsPerDepth();
    }

    mergeWith(newTree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        //TODO: (merge on existing DTO!)
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
        this.itemsAsDepth.clear();
    }

    private filterVisibleItems(documents: documentItem[]) {
        //TODO: for now display all
        const filterAtDepth = (depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage, aggregation: pageItem) => {
            if (!this.itemsAsDepth.has(depth)) {
                this.itemsAsDepth.set(depth, []);
            }

            const items = this.itemsAsDepth.get(depth);
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
                    matchingDoc.connectedPages.push(item);
                });
            }
        };

        filterAtDepth(0, this.tree.Root, null);
    }

    private layout() {
        this.calculateTreeDimensions();

        const maxItems = d3.max(this.itemsCountAtDepth);
        const pagesTotalWidth = this.getPagesOnlyWidth(maxItems);

        let yStart = reduceTreeItem.margins.treeMargin +
            reduceTreeItem.margins.titleHeight +
            reduceTreeItem.margins.treeMargin;

        const yOffset = pageItem.pageHeight +
            reduceTreeItem.margins.betweenPagesVerticalPadding;

        for (let depth = 0; depth < this.depth; depth++) {
            const items = this.itemsAsDepth.get(depth);

            const startAndOffset = graphHelper
                .computeStartAndOffset(pagesTotalWidth,
                    items.length,
                    pageItem.pageWidth);

            const xOffset = startAndOffset.offset;

            let xStart = reduceTreeItem.margins.treeMargin +
                visualizerGraphGlobal.totalEntriesWidth(maxItems) +
                reduceTreeItem.margins.treeMargin +
                this.extraLeftPadding +
                startAndOffset.start;

            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                item.y = yStart;
                item.x = xStart;
                xStart += xOffset;
            }

            yStart += yOffset;
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

        const maxItems = d3.max(this.itemsCountAtDepth);

        let width = reduceTreeItem.margins.treeMargin +
            visualizerGraphGlobal.totalEntriesWidth(maxItems) +
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
        return this.name.length * 6;
    }

    private getPagesOnlyWidth(maxItems: number) {
        return maxItems * pageItem.pageWidth +
            (maxItems + 1) * reduceTreeItem.margins.betweenPagesHorizontalPadding;
    }
}

class documentItem {
    static readonly margins = {
        minMarginBetweenDocumentNames: 30,
        badgePadding: 30
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


class visualizerGraphGlobal {

    static margins = {
        betweenPagesWidth: 30,
        betweenPagesAndDocuments: 80,
        globalMargin: 30
    }

    static readonly documentColors = ["#7cb82f", "#1a858e", "#ef6c5a"];
    private nextColorIndex = 0;

    private totalWidth = 1500; //TODO: use dynamic value
    private totalHeight = 700; //TODO: use dynamic value

    private documents = [] as Array<documentItem>;
    private reduceTrees: Array<reduceTreeItem> = [];

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;

    private dataWidth = 0; // total width of all virtual elements
    private dataHeight = 0; // total heigth of all virtual elements

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    addTrees(documentName: string, result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        const document = new documentItem(documentName);
        document.color = this.getNextColor();
        this.documents.push(document);

        for (let i = 0; i < result.length; i++) {
            const reduceTree = result[i];
            const existingTree = this.reduceTrees.find(x => x.name === reduceTree.Name);
            if (existingTree) {
                existingTree.mergeWith(reduceTree);
            } else {
                const newTree = new reduceTreeItem(reduceTree);
                this.reduceTrees.push(newTree);
            }
        }

        this.layout();
        this.zoomToDocument(document);
    }

    private getNextColor() {
        const color = visualizerGraphGlobal.documentColors[this.nextColorIndex % visualizerGraphGlobal.documentColors.length];
        this.nextColorIndex++;
        return color;
    }

    private zoomToDocument(document: documentItem) {
        const requestedTranslation: [number, number] = [-document.x + this.totalWidth / 2 - document.width / 2, 0];

        this.canvas
            .transition()
            .duration(500)
            .call(this.zoom.translate(requestedTranslation).scale(1).event);

        //TODO: end can use .each("end", listener) to get info about completion
    }

    init() {
        const container = d3.select("#visualizerContainer");

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

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight)
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<void>) {
        //TODO: setup on click, etc
        selection.on("dblclick.zoom", null);
    }

    private onZoom() {
        this.draw();
    }

    private cleanLayoutCache() {
        for (let i = 0; i < this.documents.length; i++) {
            this.documents[i].connectedPages = [];
        }
    }

    private layout() {
        this.cleanLayoutCache();

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
            + 100 //TODO: it is space for document names
            + visualizerGraphGlobal.margins.globalMargin; // top and bottom margin
        this.dataHeight = height;

        this.layoutDocuments(documentNamesYStart);
    }

    private layoutDocuments(yStart: number) {
        let totalWidth = 0;

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            const documentNameWidthEstimation = (text: string) => text.length * 9;

            doc.width = documentItem.margins.badgePadding * 2 + documentNameWidthEstimation(doc.name);
            doc.height = 35;
            doc.y = yStart;

            totalWidth += doc.width;
        }

        totalWidth += this.documents.length * (documentItem.margins.minMarginBetweenDocumentNames + 1);

        let extraItemPadding = 0;

        if (totalWidth > this.dataWidth) {
            //TODO: handle me!
        } else {
            extraItemPadding = (this.dataWidth - totalWidth) / (this.documents.length + 1);
        }

        let currentX = documentItem.margins.minMarginBetweenDocumentNames + extraItemPadding;

        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];
            doc.x = currentX;

            currentX += doc.width + documentItem.margins.minMarginBetweenDocumentNames + extraItemPadding;
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
    }

    private drawTree(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {
        ctx.fillStyle = "#2c3333";
        ctx.fillRect(tree.x, tree.y, tree.width, tree.height);

        ctx.save();
        try {
            ctx.translate(tree.x, tree.y);

            // text
            ctx.font = "10px Lato";
            ctx.textAlign = "center";
            ctx.fillStyle = "#686f6f";
            ctx.fillText(tree.name, tree.width / 2, reduceTreeItem.margins.treeMargin, tree.width);

            // total entries
            let totalEntiresY = reduceTreeItem.margins.treeMargin +
                reduceTreeItem.margins.titleHeight +
                reduceTreeItem.margins.treeMargin;
            const totalEntriesOffset = pageItem.pageHeight +
                reduceTreeItem.margins.betweenPagesVerticalPadding;

            for (let i = 1; i < tree.depth; i++) {
                totalEntiresY += totalEntriesOffset;
                const items = tree.itemsCountAtDepth[i];
                ctx.font = "18px Lato";
                ctx.textAlign = "right";
                ctx.fillText(items.toString(), reduceTreeItem.margins.treeMargin + visualizerGraphGlobal.totalEntriesWidth(items), totalEntiresY + 18 /* font size */);

                //TODO: draw vertical line
            }

            this.drawPages(ctx, tree);
        } finally {
            ctx.restore();
        }
    }

    private drawPages(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {
        ctx.fillStyle = "#008cc9";
        ctx.strokeStyle = "#686f6f";

        tree.itemsAsDepth.forEach(globalItems => {
            for (let i = 0; i < globalItems.length; i++) {
                const item = globalItems[i];
                ctx.fillRect(item.x, item.y, pageItem.pageWidth, pageItem.pageHeight);

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
        for (let i = 0; i < this.documents.length; i++) {
            const doc = this.documents[i];

            ctx.strokeStyle = doc.color;

            for (let j = 0; j < doc.connectedPages.length; j++) {
                const page = doc.connectedPages[j];

                graphHelper.drawBezierDiagonal(ctx, doc.getSourceConnectionPoint(), page.getGlobalTargetConnectionPoint(), true);
            }
        }
    }

    private drawDocument(ctx: CanvasRenderingContext2D, docItem: documentItem) {
        ctx.fillStyle = docItem.color; 
        ctx.fillRect(docItem.x, docItem.y, docItem.width, docItem.height);
        ctx.textAlign = "center";
        ctx.textBaseline = "middle"; 
        ctx.font = "18px Lato";
        ctx.fillStyle = "black";
        ctx.fillText(docItem.name, docItem.x + docItem.width / 2, docItem.y + docItem.height / 2);
    }

    static totalEntriesWidth(entries: number) {
        return Math.ceil(Math.log10(entries)) * 10;
    }
}

export = visualizerGraphGlobal;
