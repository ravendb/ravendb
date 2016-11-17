import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import getIndexesPerformance = require("commands/database/debug/getIndexesPerformance");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndex" | "trackItem";
    arg: any;
}

class hitTest {
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleIndex: (indexName: string) => void;
    private handleTooltip: (item: Raven.Client.Data.Indexes.IndexingPerformanceOperation, x: number, y: number) => void;
    private onTrackClicked: (item: Raven.Client.Data.Indexes.IndexingPerformanceOperation) => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleIndex: (indeName: string) => void,
        handleTooltip: (item: Raven.Client.Data.Indexes.IndexingPerformanceOperation, x: number, y: number) => void,
        onTrackClicked: (item: Raven.Client.Data.Indexes.IndexingPerformanceOperation) => void) {
        this.container = container;
        this.onToggleIndex = onToggleIndex;
        this.handleTooltip = handleTooltip;
        this.onTrackClicked = onTrackClicked;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Client.Data.Indexes.IndexingPerformanceOperation) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "trackItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerIndexToggle(x: number, y: number, width: number, height: number, indexName: string) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleIndex",
            arg: indexName
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    onClick() {
        const clickLocation = d3.mouse(this.container.node());

        if ((d3.event as any).defaultPrevented) {
            return;
        }

        const items = this.findItems(clickLocation[0], clickLocation[1]);

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            if (item.actionType === "toggleIndex") {
                this.onToggleIndex(item.arg as string);
            } else if (item.actionType === "trackItem") {
                this.onTrackClicked(item.arg as Raven.Client.Data.Indexes.IndexingPerformanceOperation);
            }
        }
    }

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Client.Data.Indexes.IndexingPerformanceOperation).first();
        this.handleTooltip(currentItem, clickLocation[0], clickLocation[1]);
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - metrics.brushSectionHeight,
            maxY: y - metrics.brushSectionHeight
        });
    }
}

class metrics extends viewModelBase { 

    static readonly brushSectionHeight = 40;
    static readonly trackHeight = 16; // height used for callstack item
    static readonly stackPadding = 1; // space between call stacks
    static readonly trackPadding = 3; // top / bottom padding between different tracks
    static readonly axisHeight = 20; 

    private data: Raven.Client.Data.Indexes.IndexPerformanceStats[] = [];
    private timeRange: [Date, Date];
    private totalWidth = 1500; //TODO: use dynamic value + bind on window resize
    private totalHeight = 700; //TODO: use dynamic value + bind on windows resize

    private indexNamesAndRecursion: Map<string, number>;
    private expandedTracks = new Set<string>();

    private isoParser = d3.time.format.iso;
    private canvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;
    private brushSection: HTMLCanvasElement; // virtual canvas for brush section
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;
    private yScale: d3.scale.Ordinal<string, number>;
    private hitTest = new hitTest();
    private tooltip: d3.Selection<Raven.Client.Data.Indexes.IndexingPerformanceOperation>;

    private color = d3.scale.category20c();
    private dialogVisible = false;

    activate(args: any): JQueryPromise<any> {
        super.activate(args);

        return new getIndexesPerformance(this.activeDatabase())
            .execute()
            .done(result => this.data = result)
    }

    attached() {
        super.attached();

        this.initCanvas();
        this.hitTest.init(this.svg,
            (indexName) => this.onToggleIndex(indexName),
            (item, x, y) => this.handleTooltip(item, x, y),
            item => this.showDialog(item));
    }

    compositionComplete() {
        super.compositionComplete();
        this.draw();
    }

    private initCanvas() {
        const metricsContainer = d3.select("#metricsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = metricsContainer
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.xBrushNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth])
            .domain([0, this.totalWidth]);

        this.xNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth])
            .domain([0, this.totalWidth]);

        this.brush = d3.svg.brush()
            .x(this.xBrushNumericScale)
            .on("brush", () => this.onBrush());

        this.zoom = d3.behavior.zoom()
            .x(this.xNumericScale)
            //TODO:.scaleExtent([1, 100]) - it is not that easy as brush resets scale/transform on zoom object!
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight - metrics.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + metrics.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        let mousePressed = false;

        const onMove = () => {
            this.hitTest.onMouseMove();
        }

        selection.on("mousemove.tip", onMove);

        selection.on("click", () => this.hitTest.onClick());

        selection
            .on("mousedown.tip", () => selection.on("mousemove.tip", null))
            .on("mouseup.tip", () => selection.on("mousemove.tip", onMove));

        selection.on("dblclick.zoom", null);
    }

    private draw() {
        this.prepareBrushSection();
        this.prepareMainSection();

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");
        context.drawImage(this.brushSection, 0, 0);

        this.drawMainSection();

        this.tooltip = d3.select(".tooltip");
    }

    private prepareBrushSection() {
        const timeRanges = this.extractTimeRanges();
        const collapsedTimeRanges = graphHelper.collapseTimeRanges(timeRanges);
        //TODO: maybe instead of collaping time range we should graph area chart with # currently indexing as y-axis
        this.timeRange = graphHelper.timeRangeFromSortedRanges(collapsedTimeRanges);

        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth;
        this.brushSection.height = metrics.brushSectionHeight;

        const context = this.brushSection.getContext("2d");

        const xBrushScale = d3.time.scale<number>()
            .range([0, this.totalWidth])
            .domain(this.timeRange);

        this.drawXaxis(context, xBrushScale);

        context.fillStyle = "#e2ebfe";
        context.strokeStyle = "#6e9cf8";
        context.lineWidth = 1;

        for (var i = 0; i < collapsedTimeRanges.length; i++) {
            const currentRange = collapsedTimeRanges[i];
            const x1 = xBrushScale(currentRange[0]);
            const x2 = xBrushScale(currentRange[1]);
            context.fillRect(x1, 18, x2 - x1, 10);
            context.strokeRect(x1, 18, x2 - x1, 10);
        }

        this.prepareBrush();
    }

    private prepareBrush() {
        const hasBrush = !!this.svg.select("g.brush").node();

        if (!hasBrush) {
            this.brushContainer = this.svg
                .append("g")
                .attr("class", "x brush");

            this.brushContainer
                .call(this.brush)
                .selectAll("rect")
                .attr("y", 1)
                .attr("height", metrics.brushSectionHeight - 2);
        }
    }

    private prepareMainSection() {
        this.indexNamesAndRecursion = this.findIndexNamesAndMaxRecursionLevel();
    }

    private constructYScale() {
        let currentOffset = metrics.trackPadding + metrics.axisHeight;
        let domain = [] as Array<string>;
        let range = [] as Array<number>;

        const indexesInfo = this.indexNamesAndRecursion;

        indexesInfo.forEach((recursion, indexName) => {
            domain.push(indexName);
            range.push(currentOffset);

            currentOffset += metrics.trackHeight + 2 * metrics.trackPadding;

            if (this.expandedTracks.has(indexName)) {
                currentOffset += recursion * (metrics.trackHeight + metrics.stackPadding);
            }
        });

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private findIndexNamesAndMaxRecursionLevel(): Map<string, number> {
        const result = new Map<string, number>();

        this.data.forEach(perfItem => {
            const recursionLevels = perfItem.Performance.map(x => this.findMaxRecursionLevel(x.Details));
            const maxRecursion = d3.max(recursionLevels);
            result.set(perfItem.IndexName, maxRecursion); //TODO: do we need this calc this - maybe it is constant value?
        });

        return result;
    }

    private findMaxRecursionLevel(node: Raven.Client.Data.Indexes.IndexingPerformanceOperation): number {
        if (node.Operations.length === 0) {
            return 1;
        }

        return 1 + d3.max(node.Operations.map(x => this.findMaxRecursionLevel(x)));
    }

    private drawXaxis(context: CanvasRenderingContext2D, scale: d3.time.Scale<number, number>) { //TODO: extract this to utils? 
        context.save();
        const tickCount = Math.floor(this.totalWidth / 300);
        const tickSize = 6;
        const ticks = scale.ticks(tickCount);
        const tickFormat = scale.tickFormat(tickCount);

        context.beginPath();
        context.moveTo(0, tickSize);
        context.lineTo(0, 0);
        context.lineTo(this.totalWidth - 1, 0);
        context.lineTo(this.totalWidth - 1, tickSize);

        ticks.forEach(x => {
            context.moveTo(scale(x), 0);
            context.lineTo(scale(x), tickSize);
        });
        context.strokeStyle = "white";
        context.stroke();

        context.textAlign = "center";
        context.textBaseline = "top";
        context.fillStyle = "white";
        ticks.forEach(x => {
            context.fillText(tickFormat(x), scale(x), tickSize);
        });
        context.restore();
    }

    private onZoom() {
        this.checkOffScale();

        this.brush.extent(this.xNumericScale.domain() as [number, number]);
        this.brushContainer
            .call(this.brush);

        this.drawMainSection();
    }

    private checkOffScale() {
        var t = (d3.event as any).translate,
            s = (d3.event as any).scale;
        var tx = t[0],
            ty = t[1];

        //TODO: http://bl.ocks.org/tommct/8116740
    }

    private onBrush() {
        this.xNumericScale.domain((this.brush.empty() ? this.xBrushNumericScale.domain() : this.brush.extent()) as [number, number]);
        this.zoom.x(this.xNumericScale);
        this.drawMainSection();
    }

    private extractTimeRanges(): Array<[Date, Date]> {
        const result = [] as Array<[Date, Date]>;
        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                const start = this.isoParser.parse(perfStat.Started);
                let end: Date;
                if (perfStat.Completed) {
                    end = this.isoParser.parse(perfStat.Completed);
                } else {
                    end = new Date(start.getTime() + perfStat.DurationInMilliseconds);
                }
                result.push([start, end]);
            });
        });

        return result;
    }

    private drawMainSection() {
        this.hitTest.reset();
        this.constructYScale();

        const xScale = d3.time.scale<number>() //TODO: put this as instance ?, use another scale to compute inversion and current time frame
            .range([0, this.totalWidth])
            .domain(this.timeRange);

        const visibleTimeFrame = this.xNumericScale.domain().map(x => xScale.invert(x)) as [Date, Date];

        xScale.domain(visibleTimeFrame);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.save();
        try {
            context.translate(0, metrics.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - metrics.brushSectionHeight);

            context.beginPath();
            context.rect(0, 0, this.totalWidth, this.totalHeight - metrics.brushSectionHeight); 
            context.clip();

            this.drawXaxis(context, xScale);
            this.drawTracks(context, xScale);
            this.drawIndexNames(context);

        } finally {
            context.restore();
        }
    }

    private drawTracks(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        //TODO: include quadTree, don't draw when index is offscreen, include vertical scroll, don't draw section if off screen
        //TODO: support not completed items
        //TODO: put hit area cache (use quadtree as well)

        const extentFunc = graphHelper.extentGenerator(xScale);

        this.data.forEach(perfStat => {
            const yStart = this.yScale(perfStat.IndexName);

            context.fillStyle = "red";
            perfStat.Performance.forEach(perf => {
                const startDate = this.isoParser.parse(perf.Started); //TODO: create cache for this to avoid parsing dates
                const x1 = xScale(startDate);
                const isOpened = this.expandedTracks.has(perfStat.IndexName);

                const yOffset = isOpened ? metrics.trackHeight + metrics.stackPadding : 0;

                this.drawStripes(context, [perf.Details], x1, yStart + (isOpened ? yOffset : 0), yOffset, extentFunc);
            });

        });
    }

    private drawStripes(context: CanvasRenderingContext2D, operations: Array<Raven.Client.Data.Indexes.IndexingPerformanceOperation>, xStart: number, yStart: number,
        yOffset: number, extentFunc: (duration: number) => number) {

        let currentX = xStart;
        for (let i = 0; i < operations.length; i++) {
            const op = operations[i];
            context.fillStyle = this.color(op.Name);

            const dx = extentFunc(op.DurationInMilliseconds);

            context.fillRect(currentX, yStart, dx, metrics.trackHeight);

            if (yOffset !== 0) { // track is opened
                this.hitTest.registerTrackItem(currentX, yStart, dx, metrics.trackHeight, op);
                if (op.Name.startsWith("Collection_")) {
                    context.fillStyle = "black";
                    const text = op.Name.substr("Collection_".length);
                    const textWidth = context.measureText(text).width
                    const truncatedText = graphHelper.truncText(text, textWidth, dx - 4);
                    if (truncatedText) {
                        context.fillText(truncatedText, currentX + 2, yStart + 11, dx - 4);
                    }
                }
            }
            
            if (op.Operations.length > 0) {
                this.drawStripes(context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc);
            }
            currentX += dx;
        }
    }

    private drawIndexNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 12;
        const textStart = 3 + 8 + 4;

        this.indexNamesAndRecursion.forEach((r, indexName) => {
            const rectWidth = context.measureText(indexName).width + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4; /* padding between arrow and text */ 

            context.fillStyle = "rgba(255, 255, 255, 0.2)";
            context.fillRect(2, yScale(indexName), rectWidth, metrics.trackHeight);
            this.hitTest.registerIndexToggle(2, yScale(indexName), rectWidth, metrics.
                trackHeight, indexName);
            context.fillStyle = "black";
            context.fillText(indexName, textStart, yScale(indexName) + textShift);
            graphHelper.drawArrow(context, 5, yScale(indexName) + 4, !this.expandedTracks.has(indexName));
        });
    }

    private onToggleIndex(indexName: string) {
        if (this.expandedTracks.has(indexName)) {
            this.expandedTracks.delete(indexName);
        } else {
            this.expandedTracks.add(indexName);
        }

        this.drawMainSection();
    }

    /*
     * Called by hitTest class on mouse move
     * show tooltip when element !== null
     * hide if it is null
     */
    private handleTooltip(element: Raven.Client.Data.Indexes.IndexingPerformanceOperation, x: number, y: number) {
        if (element && !this.dialogVisible) {
            const currentDatum = this.tooltip.datum();
            if (currentDatum !== element) {
                this.tooltip.transition()
                    .duration(200)
                    .style("opacity", 1);
                let html = element.Name;
                html += "<br />Duration: " + element.DurationInMilliseconds + " ms"; //TODO: format time time to avoid 2000230434 ms

                this.tooltip.html(html);
                this.tooltip.datum(element);
            }

            this.tooltip
                .style("left", x + "px")
                .style("top", (y - 38) + "px");
        } else {
            this.hideTooltip();
        }
    }

    private hideTooltip() {
        this.tooltip.datum(null);
        this.tooltip.transition()
            .duration(200)
            .style("opacity", 0);
    }

    private showDialog(element: Raven.Client.Data.Indexes.IndexingPerformanceOperation) {
        const dialog = new tempStatDialog(element, (key, value) => {
            if (key === "Operations") {
                return undefined;
            }
            return value;
        });

        this.dialogVisible = true;
        app.showBootstrapDialog(dialog)
            .always(() => this.dialogVisible = false);
        this.hideTooltip();
    }

    fileSelected() { //TODO:
        const fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function() {
// ReSharper disable once SuspiciousThisUsage
            self.dataImported(this.result);
        };
        reader.onerror = function(error: any) {
            alert(error);
        };
        reader.readAsText(file);
    }

    private dataImported(result: string) {
        this.data = JSON.parse(result);

        this.draw();
    }

    exportAsJson() {
        fileDownloader.downloadAsJson(this.data, "perf.json", "perf");
    }
}

export = metrics; 
 
