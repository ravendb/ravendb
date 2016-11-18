import app = require("durandal/app");
import graphHelper = require("common/helpers/graph/graphHelper");

import d3 = require('d3');

class globalPageItem {
    x: number;
    y: number;
}

class reduceTreeItem {

    static readonly globalMargins = {
        treeMargin: 20,
        pageWidth: 20,
        pageHeight: 20,
        betweenPagesHorizontalPadding: 13,
        betweenPagesVerticalPadding: 50,
        titleHeight: 20
    }

    private tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree;
    name: string;
    depth: number;
    itemsCountAtDepth: Array<number>;
    itemsAsDepth = new Map<number, Array<globalPageItem>>();

    width = 0;
    height = 0;
    x: number;
    y: number;
    extraLeftPadding = 0; //used when text is longer than contents

    constructor(tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        this.tree = tree;
        this.name = tree.Name;
        this.depth = tree.Depth;

        this.calcItemsAtDepth();
    }

    mergeWith(newTree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        //TODO: 
    }

    private calcItemsAtDepth() {
        this.itemsCountAtDepth = new Array(this.depth);
        this.countEntires(0, this.tree.Root);
    }

    private countEntires(depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) {
        this.itemsCountAtDepth[depth] = (this.itemsCountAtDepth[depth] || 0) + 1;

        if (node.Children && node.Children.length) {
            for (let i = 0; i < node.Children.length; i++) {
                this.countEntires(depth + 1, node.Children[i]);
            }
        }
    }

    filterAndLayoutVisibleItems(documentIds: Array<string>) {
        this.filterVisibleItems(documentIds);
        this.calculatePageWidthAndHeight();
        this.layoutVisibleItems();
    }

    private filterVisibleItems(documentIds: Array<string>) {
        //TODO: for now display all
        const filterAsDepth = (depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage) => {
            if (!this.itemsAsDepth.has(depth)) {
                this.itemsAsDepth.set(depth, []);
            }

            const items = this.itemsAsDepth.get(depth);
            const item = new globalPageItem();
            items.push(item);

            if (node.Children && node.Children.length) {
                for (let i = 0; i < node.Children.length; i++) {
                    filterAsDepth(depth + 1, node.Children[i]);
                }
            }

        };

        filterAsDepth(0, this.tree.Root);
    }

    private calculatePageWidthAndHeight() {
        let height = reduceTreeItem.globalMargins.treeMargin +
            reduceTreeItem.globalMargins.titleHeight +
            reduceTreeItem.globalMargins.treeMargin;

        height += (this.depth * reduceTreeItem.globalMargins.pageHeight) +
            (this.depth - 1) * reduceTreeItem.globalMargins.betweenPagesVerticalPadding;

        height += reduceTreeItem.globalMargins.treeMargin;

        this.height = height;

        const maxItems = d3.max(this.itemsCountAtDepth);

        let width = reduceTreeItem.globalMargins.treeMargin +
            visualizerGraph.totalEntriesWidth(maxItems) +
            reduceTreeItem.globalMargins.treeMargin;

        width += this.getTotalPagesWidth(maxItems);

        width += reduceTreeItem.globalMargins.treeMargin;

        const estimatedTextWidth = this.estimateReduceKeyWidth() + 2 * reduceTreeItem.globalMargins.treeMargin;

        if (estimatedTextWidth > width) {
            this.extraLeftPadding = (estimatedTextWidth - width) / 2;
            width = estimatedTextWidth;
        }

        this.width = width;
    }

    private layoutVisibleItems() {
        const maxItems = d3.max(this.itemsCountAtDepth);
        const pagesTotalWidth = this.getTotalPagesWidth(maxItems);

        let yStart = reduceTreeItem.globalMargins.treeMargin +
            reduceTreeItem.globalMargins.titleHeight +
            reduceTreeItem.globalMargins.treeMargin;

        const yOffset = reduceTreeItem.globalMargins.pageHeight +
            reduceTreeItem.globalMargins.betweenPagesVerticalPadding;

        for (let depth = 0; depth < this.depth; depth++) {
            const items = this.itemsAsDepth.get(depth);

            const startAndOffset = graphHelper
                .computeStartAndOffset(pagesTotalWidth,
                    items.length,
                    reduceTreeItem.globalMargins.pageWidth);

            const xOffset = startAndOffset.offset;

            let xStart = reduceTreeItem.globalMargins.treeMargin +
                visualizerGraph.totalEntriesWidth(maxItems) +
                reduceTreeItem.globalMargins.treeMargin +
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

    private estimateReduceKeyWidth() {
        return this.name.length * 6;
    }

    private getTotalPagesWidth(maxItems: number) {
        return maxItems * reduceTreeItem.globalMargins.pageWidth +
            (maxItems + 1) * reduceTreeItem.globalMargins.betweenPagesHorizontalPadding;
    }
}


class visualizerGraph {

    static globalMargins = {
        betweenPagesWidth: 30,
        betweenPagesAndDocuments: 30,
        globalMargin: 30
    }

    private totalWidth = 1500; //TODO: use dynamic value
    private totalHeight = 700; //TODO: use dynamic value

    private documentNames: Array<string> = [];
    private reduceTrees: Array<reduceTreeItem> = [];

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>;
    private zoom: d3.behavior.Zoom<void>;

    private dataWidth = 0;
    private dataHeight = 0;

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    constructor() {
        (window as any).g = this; //TODO: delete me - temporary for easy debugging
    }

    addTrees(documentName: string, result: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        this.documentNames.push(documentName);

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

        this.recalculate();
        this.draw();
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

    private recalculate() {
        let width = visualizerGraph.globalMargins.globalMargin;
        for (let i = 0; i < this.reduceTrees.length; i++) {
            const tree = this.reduceTrees[i];
            tree.filterAndLayoutVisibleItems(this.documentNames);
            tree.x = width;
            tree.y = visualizerGraph.globalMargins.globalMargin; //TODO: should we do this based on height on element?

            width += tree.width;
            width += visualizerGraph.globalMargins.betweenPagesWidth;
        }

        width -= visualizerGraph.globalMargins.betweenPagesWidth;
        width += visualizerGraph.globalMargins.globalMargin;

        this.dataWidth = width;

        let height = d3.max(this.reduceTrees, x => x.height);
        height += visualizerGraph.globalMargins.betweenPagesAndDocuments;

        height += 100; //TODO: it is space for document names
        height += 2 * visualizerGraph.globalMargins.globalMargin;
        this.dataHeight = height;
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
            ctx.fillText(tree.name, tree.width / 2, reduceTreeItem.globalMargins.treeMargin, tree.width);

            // total entries
            let totalEntiresY = reduceTreeItem.globalMargins.treeMargin +
                reduceTreeItem.globalMargins.titleHeight +
                reduceTreeItem.globalMargins.treeMargin;
            const totalEntriesOffset = reduceTreeItem.globalMargins.pageHeight +
                reduceTreeItem.globalMargins.betweenPagesVerticalPadding;

            for (let i = 1; i < tree.depth; i++) {
                totalEntiresY += totalEntriesOffset;
                const items = tree.itemsCountAtDepth[i];
                ctx.font = "18px Lato";
                ctx.textAlign = "right";
                ctx.fillText(items.toString(), reduceTreeItem.globalMargins.treeMargin + visualizerGraph.totalEntriesWidth(items), totalEntiresY + 18 /* font size */);

                //TODO: draw vertical line
            }

            this.drawPages(ctx, tree);
        } finally {
            ctx.restore();
        }
    }

    private drawPages(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {
        ctx.fillStyle = "#008cc9";
        tree.itemsAsDepth.forEach(globalItems => {
            for (let i = 0; i < globalItems.length; i++) {
                const item = globalItems[i];
                ctx.fillRect(item.x, item.y, reduceTreeItem.globalMargins.pageWidth, reduceTreeItem.globalMargins.pageHeight);
            }
        });

    }

    static totalEntriesWidth(entries: number) {
        return Math.ceil(Math.log10(entries)) * 10;
    }
}

export = visualizerGraph;
