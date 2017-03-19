import graphHelper = require("common/helpers/graph/graphHelper");

import d3 = require('d3');

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

        if (typeof(value) === "number") {
            return value.toLocaleString();
        }

        if (typeof(value) === "string") {
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
    drawOffset: number;

    connectedEntries = [] as entryItem[];

    constructor(name: string) {
        super();
        this.name = name;
    }

    get visible() {
        return this.connectedEntries.length;
    }

    getSourceConnectionPoint(): [number, number] {
        return [this.x + this.width / 2, this.y];
    }

    reset() {
        this.connectedEntries = [];
    }
}

class entryItem extends layoutableItem {
    data: Object;
    dataAsString: string;
    source: string;
    parent: pageItem;

    constructor(source: string, data: Object) {
        super();
        this.data = data;
        this.source = source;
        this.dataAsString = reduceValuesFormatter.formatData(data);
    }

    static estimateTextWidth(text: string) {
        return text.length * 6;
    }

    getGlobalTargetConnectionPoint(): [number, number] {
        return [this.x + this.parent.x, this.y + this.parent.y + this.height / 2];
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
        pageNumberTopMargin: 12,
        entryHeight: 22, 
        betweenEntryPadding: 1,
        bottomPadding: 5,
        entryTextPadding: 5,
        aggregationItemHeight: 16,
        aggragationTextHorizontalPadding: 10,
        betweenPagesMinWidth: 40,
        entriesAndAggregationTextHeight: 20,

        nestedSection: {
            width: 58, 
            height: 31,
            rightMargin: 7,
            topMargin: 42
        }
    }

    parentPage?: branchPageItem;
    pageNumber: number;

    aggregationResult: any;
    aggregationResultAsMap: Map<string, string>;

    constructor(parentPage: branchPageItem, pageNumber: number, aggregationResult: any) {
        super();

        this.pageNumber = pageNumber;
        this.parentPage = parentPage;
        this.aggregationResult = aggregationResult;
        this.aggregationResultAsMap = reduceValuesFormatter.extractValues(aggregationResult);
    }

    layout() {
        this.height = pageItem.margins.pageNumberTopMargin + pageItem.margins.entriesAndAggregationTextHeight
            + pageItem.margins.aggregationItemHeight * this.aggregationResultAsMap.size
            + pageItem.margins.bottomPadding;

        const longestTextWidth = pageItem.estimateAggregationTextWidth(this.findLongestAggregationTextLength());

        this.width = pageItem.margins.aggragationTextHorizontalPadding + longestTextWidth + pageItem.margins.aggragationTextHorizontalPadding;
    }

    private findLongestAggregationTextLength(): number {
        let max = 0;

        this.aggregationResultAsMap.forEach((v, k) => {
            const len = k.length + 2 + v.length;
            if (len > max) {
                max = len;
            }
        });

        return max;
    }

    private static estimateAggregationTextWidth(textLength: number) {
        return textLength * 6;
    }

    getSourceConnectionPoint(): [number, number] {
        return [this.x + this.width / 2, this.y];
    }

    getTargetConnectionPoint(): [number, number] {
        return [this.x + this.width / 2, this.y + this.height];
    }
    
}

class leafPageItem extends pageItem {

    entriesTextY: number;

    nestedSection: boolean;

    entries = [] as Array<entryItem | entryPaddingItem>;

    constructor(parentPage: branchPageItem, pageNumber: number, aggregationResult: any, nestedSection: boolean, entries: Array<entryItem | entryPaddingItem>) {
        super(parentPage, pageNumber, aggregationResult);
        this.nestedSection = nestedSection;
        this.entries = entries;
    }

    layout() {
        super.layout();

        const entriesData = this.entries
            .filter(x => x instanceof entryItem)
            .map(x => (x as entryItem).dataAsString)
            .sort((a, b) => a.length > b.length ? -1 : 1);

        const longestText = entriesData[0];
        const longestTextWidth = longestText ? entryItem.estimateTextWidth(longestText) : entryPaddingItem.margins.minWidth;

        const entriesWidth = pageItem.margins.horizonalPadding + pageItem.margins.entryTextPadding + longestTextWidth + pageItem.margins.entryTextPadding + pageItem.margins.horizonalPadding;
        this.width = Math.max(entriesWidth, this.width);

        this.height -= pageItem.margins.bottomPadding;

        this.entriesTextY = this.height;

        let yStart = this.height + pageItem.margins.entriesAndAggregationTextHeight;
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
                const entry = new entryItem(entries[i].Source, entries[i].Data);
                if (entry.source) {
                    const matchedDocument = documents.find(x => x.name === entry.source);
                    if (matchedDocument) {
                        matchedDocument.connectedEntries.push(entry);
                    }
                }
                result.push(entry);
            } else {
                if (result.length === 0 || _.last(result) instanceof entryItem) {
                    result.push(new entryPaddingItem());
                }
            }
        }

        return result;
    }
}

class branchPageItem extends pageItem {

    constructor(parentPage: branchPageItem, pageNumber: number, aggregationResult: any) {
        super(parentPage, pageNumber, aggregationResult);
    }
    
}

class reduceTreeItem {
    private tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree;

    totalWidth: number;
    totalHeigh: number;

    displayName: string;
    depth: number;
    itemsCountAtDepth: Array<number>; // this represents non-filtered count
    itemsAtDepth = new Map<number, Array<pageItem>>(); // items after filtering depth -> list of items

    constructor(tree: Raven.Server.Documents.Indexes.Debugging.ReduceTree) {
        this.tree = tree;
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

    filterAndLayoutVisibleItems(documents: documentItem[]): number {
        this.cleanCache(documents);
        this.filterVisibleItems(documents);
        return this.layout();
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
                const isNestedSection = depth === 0; // if depth is zero and node has entries
                const item = new leafPageItem(parentPage, node.PageNumber, node.AggregationResult, isNestedSection, entries);
                entries.filter(x => x instanceof entryItem).forEach((entry: entryItem) => entry.parent = item);
                items.push(item);
            }
        };

        filterAtDepth(0, this.tree.Root, null);
    }

    private layout(): number {
        this.itemsAtDepth.forEach((pages, depth) => {
            pages.forEach(page => page.layout());
        });

        const lastLevelItems = this.itemsAtDepth.get(this.depth - 1);

        // make all items at last level at the same size
        const maxWidth = d3.max(lastLevelItems, x => x.width);
        for (let i = 0; i < lastLevelItems.length; i++) {
            lastLevelItems[i].width = maxWidth;
        }

        this.totalWidth = lastLevelItems.reduce((p, c) => p + c.width, 0)
            + (lastLevelItems.length + 1) * pageItem.margins.betweenPagesMinWidth;

        let yStart = visualizerGraphDetails.margins.top;

        const avgElementWidthPerDepth = Array.from(this.itemsAtDepth.values()).map(pages => {
            const totalWidth = pages.reduce((p, c) => p + c.width, 0);
            return totalWidth / pages.length;
        });

        const maxHeightPerLevel = Array.from(this.itemsAtDepth.values()).map(pages => d3.max(pages, x => x.height));

        for (let depth = 0; depth < this.depth; depth++) {
            const items = this.itemsAtDepth.get(depth);

            const startAndOffset = graphHelper.computeStartAndOffset(this.totalWidth, items.length,
                avgElementWidthPerDepth[depth]);

            const xOffset = startAndOffset.offset;

            let xStart = startAndOffset.start;

            for (let i = 0; i < items.length; i++) {
                const item = items[i];
                item.x = xStart;
                item.y = yStart;

                xStart += xOffset;
            }

            yStart += maxHeightPerLevel[depth] + visualizerGraphDetails.margins.verticalMarginBetweenLevels;
        }

        return yStart;
    }
}


class visualizerGraphDetails {

    static margins = {
        top: 40,
        verticalMarginBetweenLevels: 60,
        betweenTreesAndDocumentsPadding: 80,
        badgePadding: 30,
        minMarginBetweenDocumentNames: 30,
        straightLine: 12,
        arrowHalfHeight: 6,
        arrowWidth: 8,
        betweenLinesOffset: 5
    }

    private totalWidth = 1500; //TODO: use dynamic value
    private totalHeight = 700; //TODO: use dynamic value

    private documents = [] as Array<documentItem>;

    private canvas: d3.Selection<void>;
    private svg: d3.Selection<void>; //TODO: do we really need svg in here?
    private zoom: d3.behavior.Zoom<void>;

    private xScale: d3.scale.Linear<number, number>;
    private yScale: d3.scale.Linear<number, number>;

    private viewActive = ko.observable<boolean>(false);
    private gotoMasterViewCallback: () => void;

    private trees: Raven.Server.Documents.Indexes.Debugging.ReduceTree[];
    private currentTreeIndex = 0;
    private currentTree: reduceTreeItem;

    private currentLineOffset = 0;
    private connectionsBaseY = 0;

    constructor() {
        _.bindAll(this, "goToMasterView" as keyof this);
    }

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

    addDocument(documentName: string) {
        const document = new documentItem(documentName);
        this.currentLineOffset += visualizerGraphDetails.margins.betweenLinesOffset;
        this.documents.push(document);
    }

    reset() {
        this.restoreView();
        this.documents = [];
    }

    setDocumentsColors(documentsColorsSetup: Array<documentColorPair>) {
        for (let i = 0; i < documentsColorsSetup.length; i++) {
            let documentItem = this.documents.find((d) => d.name === documentsColorsSetup[i].docName);
            documentItem.color = documentsColorsSetup[i].docColor;
        }
    }

    private restoreView() {
        this.zoom.translate([0, 0]).scale(1).event(this.canvas);
        this.documents.forEach(doc => doc.reset());
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
        this.restoreView();

        this.viewActive(true); //TODO: consider setting this after initial animation if any
        this.toggleUiElements(true);

        const treeIdx = this.trees.findIndex(x => x.Name === treeName);
        this.currentTreeIndex = treeIdx;
        this.currentTree = new reduceTreeItem(this.trees[this.currentTreeIndex]); //TODO: consider moving this to layout?

        this.layout();

        const initialTranslation: [number, number] = [this.totalWidth / 2 - this.currentTree.totalWidth / 2, 0];
        this.zoom.translate(initialTranslation).scale(1).event(this.canvas);

        this.draw();
    }

    private layout() {
        let yStart = this.currentTree.filterAndLayoutVisibleItems(this.documents);

        const visibleDocuments = this.getVisibleDocuments();

        this.connectionsBaseY = yStart
            + visualizerGraphDetails.margins.betweenTreesAndDocumentsPadding / 2
            - (visualizerGraphDetails.margins.betweenLinesOffset * visibleDocuments.length) / 2;

        yStart += visualizerGraphDetails.margins.betweenTreesAndDocumentsPadding;

        this.layoutDocuments(yStart);
    }

    // Return the visible items for the detailed view, ordered by their connectedEntries y coordinate
    getVisibleDocuments(): documentItem[] {
        return this.documents.filter(x => x.visible).sort((a, b) => { return a.connectedEntries[0].y - b.connectedEntries[0].y });
    }

    // set x & y of document & the draw offsets
    private layoutDocuments(yStart: number) {
        let totalWidth = 0;

        const visibleDocuments = this.getVisibleDocuments();

        for (let i = 0; i < visibleDocuments.length; i++) {
            const doc = visibleDocuments[i];

            const documentNameWidthEstimation = (text: string) => text.length * 9;

            doc.width = visualizerGraphDetails.margins.badgePadding * 2 + documentNameWidthEstimation(doc.name);
            doc.height = 35;
            doc.y = yStart;

            totalWidth += doc.width;
        }

        totalWidth += visibleDocuments.length * (visualizerGraphDetails.margins.minMarginBetweenDocumentNames + 1);

        let extraItemPadding = 0;

        if (totalWidth > this.currentTree.totalWidth) {
            //TODO: handle me!
        } else {
            extraItemPadding = (this.currentTree.totalWidth - totalWidth) / (visibleDocuments.length + 1);
        }

        // if there are many documents to draw than we better start with a lower y coordinate
        let yOffsetOfHorizontalLine = visibleDocuments.length > 10 ?  20 : 0;

        let currentX = visualizerGraphDetails.margins.minMarginBetweenDocumentNames + extraItemPadding;
        for (let i = 0; i < visibleDocuments.length; i++) {
            const doc = visibleDocuments[i];

            doc.x = currentX;
            currentX += doc.width + visualizerGraphDetails.margins.minMarginBetweenDocumentNames + extraItemPadding;

            doc.drawOffset = yOffsetOfHorizontalLine;
            yOffsetOfHorizontalLine -= visualizerGraphDetails.margins.betweenLinesOffset;
        }
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

            const visibleDocuments = this.getVisibleDocuments();

            for (let i = 0; i < visibleDocuments.length; i++) {
                const doc = visibleDocuments[i];
                this.drawDocument(ctx, doc);
            }

            this.drawDocumentConnections(ctx);

        } finally {
            ctx.restore();
        }
    }

    private drawTree(ctx: CanvasRenderingContext2D, tree: reduceTreeItem) {
        tree.itemsAtDepth.forEach(pages => {
            for (let i = 0; i < pages.length; i++) {
                const page = pages[i];
                this.drawPage(ctx, page);

                if (page.parentPage) {
                    ctx.strokeStyle = "#686f6f";
                    ctx.lineWidth = 2;

                    const sourcePoint = page.getSourceConnectionPoint();
                    const targetPoint = page.parentPage.getTargetConnectionPoint();

                    const middleY = (sourcePoint[1] + targetPoint[1]) / 2;

                    ctx.beginPath();
                    ctx.moveTo(sourcePoint[0], sourcePoint[1]);
                    ctx.lineTo(sourcePoint[0], middleY);
                    ctx.lineTo(targetPoint[0], middleY);
                    ctx.lineTo(targetPoint[0], targetPoint[1]);
                    ctx.stroke();

                    ctx.beginPath();
                    ctx.moveTo(targetPoint[0] - visualizerGraphDetails.margins.arrowWidth, targetPoint[1] + visualizerGraphDetails.margins.arrowHalfHeight);
                    ctx.lineTo(targetPoint[0], targetPoint[1]);
                    ctx.lineTo(targetPoint[0] + visualizerGraphDetails.margins.arrowWidth, targetPoint[1] + visualizerGraphDetails.margins.arrowHalfHeight);
                    ctx.stroke();
                }
            }
        });
    }

    private drawPage(ctx: CanvasRenderingContext2D, page: pageItem) {
        ctx.fillStyle = "#3a4242";
        ctx.fillRect(page.x, page.y, page.width, page.height);

        ctx.save();
        ctx.translate(page.x, page.y);
        try {

            // page number
            ctx.fillStyle = "#008cc9";
            ctx.font = "bold 24px Lato";
            ctx.textAlign = "right";
            ctx.textBaseline = "top";
            ctx.fillText("#" + page.pageNumber, page.width - pageItem.margins.aggragationTextHorizontalPadding, pageItem.margins.pageNumberTopMargin);

            if (page instanceof leafPageItem && page.nestedSection) {
                const nestedSectionMargins = pageItem.margins.nestedSection;
                ctx.fillStyle = "#008cc9";
                ctx.fillRect(page.width - nestedSectionMargins.rightMargin - nestedSectionMargins.width, nestedSectionMargins.topMargin, nestedSectionMargins.width, nestedSectionMargins.height);

                ctx.textAlign = "center";
                ctx.fillStyle = "white";
                ctx.font = "11px Lato";
                ctx.fillText("NESTED", page.width - nestedSectionMargins.rightMargin - nestedSectionMargins.width / 2, nestedSectionMargins.topMargin + 3);
                ctx.fillText("SECTION", page.width - nestedSectionMargins.rightMargin - nestedSectionMargins.width / 2, nestedSectionMargins.topMargin + 15);
            }
           
            this.drawAggregation(ctx, page);

            if (page instanceof leafPageItem) {
                this.drawEntries(ctx, page);
            }
            
        } finally {
            ctx.restore();
        }
    }

    private drawEntries(ctx: CanvasRenderingContext2D, leaf: leafPageItem) {
        const entries = leaf.entries;
        ctx.font = "11px Lato";
        ctx.fillStyle = "#626969";
        ctx.textAlign = "left";
        ctx.textBaseline = "top";
        ctx.fillText("Entries:", pageItem.margins.aggragationTextHorizontalPadding, leaf.entriesTextY);

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

    private drawAggregation(ctx: CanvasRenderingContext2D, branch: branchPageItem) {
        ctx.font = "11px Lato";
        ctx.fillStyle = "#626969";
        ctx.textAlign = "left";
        ctx.textBaseline = "top";
        ctx.fillText("Aggregation:", pageItem.margins.aggragationTextHorizontalPadding, pageItem.margins.pageNumberTopMargin);

        ctx.fillStyle = "#a9adad";
        ctx.font = "12px Lato";
        ctx.textAlign = "left";

        let currentY = pageItem.margins.pageNumberTopMargin + pageItem.margins.entriesAndAggregationTextHeight - 5;
        const yOffset = pageItem.margins.aggregationItemHeight;

        branch.aggregationResultAsMap.forEach((value, key) => {
            ctx.fillText(key + ": " + value, pageItem.margins.aggragationTextHorizontalPadding, currentY);
            currentY += yOffset;
        });
    }

    private drawDocument(ctx: CanvasRenderingContext2D, docItem: documentItem) {
        if (docItem.visible) {
            //TODO: it is the same as in global - consider merging?
            ctx.fillStyle = docItem.color;
            ctx.fillRect(docItem.x, docItem.y, docItem.width, docItem.height);
            ctx.textAlign = "center";
            ctx.textBaseline = "middle";
            ctx.font = "18px Lato";
            ctx.fillStyle = "black";
            ctx.fillText(docItem.name, docItem.x + docItem.width / 2, docItem.y + docItem.height / 2);
        }
    }

    private drawDocumentConnections(ctx: CanvasRenderingContext2D) {
        ctx.lineWidth = 2;
        const visibleDocuments = this.getVisibleDocuments();

        for (let i = 0; i < visibleDocuments.length; i++) {
            const doc = visibleDocuments[i];

            ctx.strokeStyle = doc.color;

            for (let j = 0; j < doc.connectedEntries.length; j++) {
                const entry = doc.connectedEntries[j];

                const source = doc.getSourceConnectionPoint();
                const target = entry.getGlobalTargetConnectionPoint();

                ctx.beginPath();
                ctx.moveTo(source[0], source[1]);
                ctx.lineTo(source[0], this.connectionsBaseY + doc.drawOffset);
                ctx.lineTo(target[0] - visualizerGraphDetails.margins.straightLine - doc.drawOffset - visualizerGraphDetails.margins.betweenLinesOffset * visibleDocuments.length,
                           this.connectionsBaseY + doc.drawOffset);
                ctx.lineTo(target[0] - visualizerGraphDetails.margins.straightLine - doc.drawOffset - visualizerGraphDetails.margins.betweenLinesOffset * visibleDocuments.length,
                           target[1]);
                ctx.lineTo(target[0], target[1]);
                ctx.stroke();

                ctx.beginPath();
                ctx.moveTo(target[0] - visualizerGraphDetails.margins.arrowWidth, target[1] - visualizerGraphDetails.margins.arrowHalfHeight);
                ctx.lineTo(target[0], target[1]);
                ctx.lineTo(target[0] - visualizerGraphDetails.margins.arrowWidth, target[1] + visualizerGraphDetails.margins.arrowHalfHeight);
                ctx.stroke();
            }
        }
    }

    private toggleUiElements(show: boolean) {
        this.svg.style("display", show ? "block" : "none");
        this.canvas.style("display", show ? "block" : "none");
    }
}

export = visualizerGraphDetails;
