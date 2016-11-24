import app = require("durandal/app");
import graphHelper = require("common/helpers/graph/graphHelper");

import d3 = require('d3');
import rbush = require("rbush");

abstract class reduceValuesFormatter {
    static formatData(data: any) {
        let output = "";

        const valuesMap = reduceValuesFormatter.extractValues(data);

        let first = true;

        valuesMap.forEach((value, key) => {
            if (!first) output += ", ";
            first = false;

            output += key + ": " + reduceValuesFormatter.formatValue(data[key]);
        });

        return output;
    }

    static formatValue(value: any): string {
        if (value === null) {
            return "null";
        }

        if (value instanceof Number) {
            return value.toString();
        }

        if (value instanceof String) {
            return value as string;
        }

        return JSON.stringify(value);
    }

    static extractValues(object: any): Map<string, string> {
        const result = new Map<string, string>();

        const keys = Object.keys(object);

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const value = reduceValuesFormatter.formatValue(object[key]);
            result.set(key, value);
        }

        return result;
    }
}

class layoutableItem {
    x: number;
    y: number;
    width: number;
    height: number;
}

class documentItem extends layoutableItem {
    name: string;
    color: string;

    constructor(name: string, color: string) {
        super();
        this.name = name;
        this.color = color;
    }
}

class entryItem extends layoutableItem {
    data: Object;
    dataAsString: string;
    source: string;

    constructor(source: string, data: Object) {
        super();
        this.data = data;
        this.source = source;
        this.dataAsString = reduceValuesFormatter.formatData(data);
    }

    static estimateTextWidth(text: string) {
        return text.length * 6;
    }
}

class entryPaddingItem extends layoutableItem {

    static margins = {
        minWidth: 130
    }

}

abstract class pageItem extends layoutableItem {

    static margins = {
        horizonalPadding: 5,
        pageNumberSectionHeight: 50,// (with top and bottom paddings)
        pageNumberTopMargin: 14,
        entryHeight: 22, 
        betweenEntryPadding: 1,
        bottomPadding: 5,
        entryTextPadding: 5,
        aggregationItemHeight: 22,
        aggragationTextHorizontalPadding: 18
    }

    parentPage?: branchPageItem;
    pageNumber: number;
    
    constructor(parentPage: branchPageItem, pageNumber: number) {
        super();
        this.pageNumber = pageNumber;
        this.parentPage = parentPage;
    }

    abstract layout(): void;
    
}

class leafPageItem extends pageItem {

    entries = [] as Array<entryItem | entryPaddingItem>;

    constructor(parentPage: branchPageItem, pageNumber: number, entries: Array<entryItem | entryPaddingItem>) {
        super(parentPage, pageNumber);
        this.entries = entries;
    }

    layout() {
        const entriesData = this.entries
            .filter(x => x instanceof entryItem)
            .map(x => (x as entryItem).dataAsString)
            .sort((a, b) => a.length > b.length ? -1 : 1);

        const longestText = entriesData[0];
        const longestTextWidth = longestText ? entryItem.estimateTextWidth(longestText) : entryPaddingItem.margins.minWidth;

        this.width = pageItem.margins.horizonalPadding + pageItem.margins.entryTextPadding + longestTextWidth + pageItem.margins.entryTextPadding + pageItem.margins.horizonalPadding;

        let yStart = pageItem.margins.pageNumberSectionHeight;
        let yOffset = pageItem.margins.betweenEntryPadding + pageItem.margins.entryHeight;

        for (let i = 0; i < this.entries.length; i++) {
            const entry = this.entries[i];
            entry.x = pageItem.margins.horizonalPadding;
            entry.y = yStart;
            entry.width = longestTextWidth + 2 * pageItem.margins.entryTextPadding;
            entry.height = pageItem.margins.entryHeight;

            yStart += yOffset;
        }

        yStart += -pageItem.margins.betweenEntryPadding + pageItem.margins.bottomPadding;

        this.height = yStart;
    }

    static findEntries(documents: documentItem[], entries: Array<Raven.Server.Documents.Indexes.Debugging.MapResultInLeaf>) {
        const documentNames = documents.map(x => x.name);

        let hasAnySource = false;

        // get elements with source and one before and ahead
        const entiresToTake = new Array(entries.length).fill(0);
        for (let i = 0; i < entries.length; i++) {
            const entry = entries[i];
            if (entry.Source) {
                hasAnySource = true;

                if (i > 0) entiresToTake[i - 1] = 1;
                entiresToTake[i] = 1;
                if (i < entries.length - 1) entiresToTake[i + 1] = 1;
            }
        }

        if (!hasAnySource) {
            // display few first items then
            for (let i = 0; i < Math.min(entries.length, 3); i++) {
                entiresToTake[i] = 1;
            }
        }

        const result = [] as Array<entryItem | entryPaddingItem>;
        for (let i = 0; i < entries.length; i++) {
            if (entiresToTake[i]) {
                result.push(new entryItem(entries[i].Source, entries[i].Data));
            } else {
                if (result.length === 0 || result.last() instanceof entryItem) {
                    result.push(new entryPaddingItem());
                }
            }
        }

        return result;
    }
}

class branchPageItem extends pageItem {

    aggregationResult: any;

    aggregationResultAsMap: Map<string, string>;

    constructor(parentPage: branchPageItem, pageNumber: number, aggregationResult: any) {
        super(parentPage, pageNumber);
        this.aggregationResult = aggregationResult;
        this.aggregationResultAsMap = reduceValuesFormatter.extractValues(aggregationResult);
    }

    layout() {
        this.height = pageItem.margins.pageNumberSectionHeight
            + pageItem.margins.aggregationItemHeight * this.aggregationResultAsMap.size
            + pageItem.margins.bottomPadding;

        const longestTextWidth = branchPageItem.estimateTextWidth(this.findLongestTextLength());

        this.width = pageItem.margins.aggragationTextHorizontalPadding + longestTextWidth + pageItem.margins.aggragationTextHorizontalPadding;
    }

    private findLongestTextLength(): number {
        let max = 0;

        this.aggregationResultAsMap.forEach((v, k) => {
            const len = k.length + 2 + v.length;
            if (len > max) {
                max = len;
            }
        });

        return max;
    }

    private static estimateTextWidth(textLength: number) {
        return textLength * 6;
    }
}

class reduceTreeItem {
    private tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree;

    displayName: string;
    depth: number;
    itemsCountAtDepth: Array<number>; // this represents non-filtered count
    itemsAtDepth = new Map<number, Array<pageItem>>(); // items after filtering depth -> list of items

    constructor(tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        this.tree = tree;
        this.displayName = tree.DisplayName;

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
        const filterAtDepth = (depth: number, node: Raven.Server.Documents.Indexes.Debugging.ReduceTreePage, parentPage: branchPageItem) => {
            if (!this.itemsAtDepth.has(depth)) {
                this.itemsAtDepth.set(depth, []);
            }

            const items = this.itemsAtDepth.get(depth);

            //TODO: can we have both in simple pages?

            if (node.Children) {
                const item = new branchPageItem(parentPage, node.PageNumber, node.AggregationResult);
                items.push(item);

                for (let i = 0; i < node.Children.length; i++) {
                    filterAtDepth(depth + 1, node.Children[i], item);
                }
            }

            if (node.Entries && node.Entries.length) {
                const entries = leafPageItem.findEntries(documents, node.Entries);
                const item = new leafPageItem(parentPage, node.PageNumber, entries);
                items.push(item);
            }
        };

        filterAtDepth(0, this.tree.Root, null);
    }

    private layout() {
        this.itemsAtDepth.forEach((pages, depth) => {
            pages.forEach(page => page.layout());
        });

        //TODO: delete this!
        this.itemsAtDepth.forEach((pages, depth) => {
            pages.forEach(page => {
                page.x = Math.random() * 1300;
                page.y = Math.random() * 600;
            });
        });


        console.log("layout me");
        //TODO: 
    }
}


class visualizerGraphDetails {

    static margins = {
       //TODO: 
    }

    private totalWidth = 1500; //TODO: use dynamic value
    private totalHeight = 700; //TODO: use dynamic value

    private documents = [] as Array<documentItem>;

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>; //TODO: do we really need svg in here?
    private zoom: d3.behavior.Zoom<void>;

    private dataWidth = 0; // total width of all virtual elements
    private dataHeight = 0; // total heigth of all virtual elements

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    private viewActive = ko.observable<boolean>(false);
    private gotoMasterViewCallback: () => void;

    private trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[];
    private currentTreeIndex = 0;
    private currentTree: reduceTreeItem;

    init(goToMasterViewCallback: () => void, trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[]) {
        this.gotoMasterViewCallback = goToMasterViewCallback;
        this.trees = trees;

        const container = d3.select("#visualizerContainer");

        this.canvas = container
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = container
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.toggleUiElements(false);

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

    addDocument(documentName: string, color: string) {
        const document = new documentItem(documentName, color);
        this.documents.push(document);
    }

    reset() {
        this.zoom.translate([0, 0]).scale(1).event(this.canvas);
        //TODO: reset documents and details index
    }

    private onZoom() {
        this.draw();
    }

    private setupEvents(selection: d3.Selection<void>) {
        selection.on("dblclick.zoom", null);
        //TODO: allow to click selection.on("click", () => this.onClick());
    }

    goToMasterView() {
        this.viewActive(false);
        this.toggleUiElements(false);
        //TODO: exit animation before calling callback
        this.gotoMasterViewCallback();
    }

    openFor(treeName: string) {
        this.reset();
        this.viewActive(true); //TODO: consider setting this after initial animation if any
        this.toggleUiElements(true);

        const treeIdx = this.trees.findIndex(x => x.Name === treeName);
        this.currentTreeIndex = treeIdx;
        this.currentTree = new reduceTreeItem(this.trees[this.currentTreeIndex]); //TODO: consider moving this to layout?

        this.layout();
        this.draw();
    }

    private layout() {
        this.currentTree.filterAndLayoutVisibleItems(this.documents);


        //TODO: layout document and this
    }

    private draw() {
        const canvas = this.canvas.node() as HTMLCanvasElement;
        const ctx = canvas.getContext("2d");

        ctx.fillStyle = "#2c3333";
        ctx.fillRect(0, 0, this.totalWidth, this.totalHeight);
        ctx.save();

        try {
            const translation = this.zoom.translate();
            ctx.translate(translation[0], translation[1]);
            ctx.scale(this.zoom.scale(), this.zoom.scale());

            if (this.currentTree) {
                this.drawTree(ctx, this.currentTree);
            }

            /* TODO
            for (let i = 0; i < this.documents.length; i++) {
                const doc = this.documents[i];
                this.drawDocument(ctx, doc);
            }
    
            this.drawDocumentConnections(ctx);*/

        } finally {
            ctx.restore();
        }
    }

    private drawTree(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {

        tree.itemsAtDepth.forEach(pages => {
            for (let i = 0; i < pages.length; i++) {
                const page = pages[i];
                this.drawPage(ctx, page);
            }
        });
    }

    private drawPage(ctx: CanvasRenderingContext2D, page: pageItem) {
        ctx.fillStyle = "#3a4242";
        ctx.fillRect(page.x, page.y, page.width, page.height);

        ctx.save();
        ctx.translate(page.x, page.y);
        try {
            ctx.fillStyle = "#008cc9";
            ctx.font = "bold 22px Lato";
            ctx.textAlign = "left";
            ctx.textBaseline = "top";

            const pageNumberLeftPadding = page instanceof leafPageItem ? pageItem.margins.horizonalPadding : pageItem.margins.aggragationTextHorizontalPadding;
            ctx.fillText("#" + page.pageNumber, pageNumberLeftPadding, pageItem.margins.pageNumberTopMargin);

            if (page instanceof leafPageItem) {
                this.drawEntries(ctx, page.entries);
            } else if (page instanceof branchPageItem) {
                this.drawBranch(ctx, page);
            }
            
        } finally {
            ctx.restore();
        }
    }

    private drawEntries(ctx: CanvasRenderingContext2D, entries: Array<entryItem | entryPaddingItem>) {
        for (let i = 0; i < entries.length; i++) {
            const entry = entries[i];
            ctx.fillStyle = "#2c3333";
            ctx.fillRect(entry.x, entry.y, entry.width, entry.height);

            ctx.fillStyle = "#a9adad";
            ctx.font = "12px Lato";
            if (entry instanceof entryPaddingItem) {
                ctx.textAlign = "center";
                ctx.fillText("...", entry.x + entry.width / 2, entry.y);
            } else {
                const castedEntry = entry as entryItem;
                ctx.textAlign = "left";
                ctx.fillText(castedEntry.dataAsString, castedEntry.x + pageItem.margins.entryTextPadding, castedEntry.y + 3);
            }
        }
    }

    private drawBranch(ctx: CanvasRenderingContext2D, branch: branchPageItem) {
        ctx.fillStyle = "#a9adad";
        ctx.font = "12px Lato";
        ctx.textAlign = "left";

        let currentY = pageItem.margins.pageNumberSectionHeight;
        const yOffset = pageItem.margins.aggregationItemHeight;

        branch.aggregationResultAsMap.forEach((value, key) => {
            ctx.fillText(key + ": " + value, pageItem.margins.aggragationTextHorizontalPadding, currentY);
            currentY += yOffset;
        });
    }

    private toggleUiElements(show: boolean) {
        this.svg.style("display", show ? "block" : "none");
        this.canvas.style("display", show ? "block" : "none");
    }
}

export = visualizerGraphDetails;
