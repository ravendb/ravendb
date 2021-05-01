import viewModelBase = require("viewmodels/viewModelBase");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");
import gapFinder = require("common/helpers/graph/gapFinder");
import generalUtils = require("common/generalUtils");
import rangeAggregator = require("common/helpers/graph/rangeAggregator");
import liveIndexPerformanceWebSocketClient = require("common/liveIndexPerformanceWebSocketClient");
import inProgressAnimator = require("common/helpers/graph/inProgressAnimator");
import messagePublisher = require("common/messagePublisher");
import getIndexesStatsCommand = require("commands/database/index/getIndexesStatsCommand");
import colorsManager = require("common/colorsManager");
import fileImporter = require("common/fileImporter");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndex" | "trackItem" | "gapItem";
    arg: any;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleIndex: (indexName: string) => void;
    private handleTrackTooltip: (item: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, x: number, y: number) => void;
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;
   
    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleIndex: (indeName: string) => void,
        handleTrackTooltip: (item: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, x: number, y: number) => void,
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {
        this.container = container;
        this.onToggleIndex = onToggleIndex;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation) {
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
   
    registerGapItem(x: number, y: number, width: number, height: number, element: timeGapInfo) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "gapItem",
            arg: element
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
                return;
            } 
        }
    }

    onMouseDown() {
        this.cursor(graphHelper.prefixStyle("grabbing"));
    }

    onMouseUp() {
        this.cursor(graphHelper.prefixStyle("grab"));
    }

    findTrackItem(position: { x: number, y: number}) {
        const items = this.findItems(position.x, position.y);
        const match = items.find(x => x.actionType === "trackItem");
        return match ? match.arg as Raven.Client.Documents.Indexes.IndexingPerformanceOperation : null;
    }
    
    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const overToggleIndex = items.filter(x => x.actionType === "toggleIndex").length > 0;

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Client.Documents.Indexes.IndexingPerformanceOperation)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
            this.cursor("auto");
        } else {
            const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
            if (currentGapItem) {
                this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
                this.cursor("auto");
            } else {
                this.cursor(overToggleIndex ? "pointer" : graphHelper.prefixStyle("grab"));
                this.removeTooltip();
            }
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - indexPerformance.brushSectionHeight,
            maxY: y - indexPerformance.brushSectionHeight
        });
    }
}

class indexPerformance extends viewModelBase {

    /* static */

    static readonly brushSectionHeight = 40;
    private static readonly brushSectionIndexesWorkHeight = 22;
    private static readonly brushSectionLineWidth = 1;
    private static readonly trackHeight = 18; // height used for callstack item
    private static readonly waitTrackPadding = 4;
    private static readonly stackPadding = 1; // space between call stacks
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly axisHeight = 35; 

    private static readonly maxRecursion = 5;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly bufferSize = 50000;


    private static readonly openedTrackHeight = indexPerformance.openedTrackPadding
    + (indexPerformance.maxRecursion + 1) * indexPerformance.trackHeight
    + indexPerformance.maxRecursion * indexPerformance.stackPadding
    + indexPerformance.openedTrackPadding;

    private static readonly closedTrackHeight = indexPerformance.closedTrackPadding
    + indexPerformance.trackHeight
    + indexPerformance.closedTrackPadding;

    /* observables */

    hasAnyData = ko.observable<boolean>(false);
    loading: KnockoutComputed<boolean>;
    private searchText = ko.observable<string>("");

    private liveViewClient = ko.observable<liveIndexPerformanceWebSocketClient>();
    private autoScroll = ko.observable<boolean>(false);
    private clearSelectionVisible = ko.observable<boolean>(false);

    private faultyIndexes = ko.observableArray<string>();
    private indexNames = ko.observableArray<string>();
    private filteredIndexNames = ko.observableArray<string>();
    private expandedTracks = ko.observableArray<string>();
    private isImport = ko.observable<boolean>(false);
    private importFileName = ko.observable<string>();

    private canExpandAll: KnockoutComputed<boolean>;

    /* private */

    private data: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = [];
    private bufferIsFull = ko.observable<boolean>(false);
    private bufferUsage = ko.observable<string>("0.0");
    private dateCutoff: Date; // used to avoid showing server side cached items, after 'clear' is clicked. 
    private totalWidth: number;
    private totalHeight: number;
    private currentYOffset = 0;
    private maxYOffset = 0;
    private hitTest = new hitTest();
    private gapFinder: gapFinder;
    private dialogVisible = false;

    private inProgressAnimator: inProgressAnimator;

    /* d3 */

    private xTickFormat = d3.time.format("%H:%M:%S");
    private canvas: d3.Selection<any>;
    private inProgressCanvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private brushAndZoomCallbacksDisabled = false;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xBrushTimeScale: d3.time.Scale<number, number>;
    private yBrushValueScale: d3.scale.Linear<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;
    private brushSection: HTMLCanvasElement; // virtual canvas for brush section
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;
    private yScale: d3.scale.Ordinal<string, number>;
    private tooltip: d3.Selection<Raven.Client.Documents.Indexes.IndexingPerformanceOperation | timeGapInfo>;
    private currentTrackTooltipPosition: { x: number, y: number} = null;
    private scrollConfig: scrollColorConfig;
    
    private colors = {
        axis: undefined as string,
        gaps: undefined as string,
        brushChartColor: undefined as string,
        brushChartStrokeColor: undefined as string,
        trackBackground: undefined as string,
        trackNameBg: undefined as string,
        faulty: undefined as string,
        itemWithError: undefined as string,
        trackNameFg: undefined as string,
        openedTrackArrow: undefined as string,
        closedTrackArrow: undefined as string,
        stripeTextColor: undefined as string,
        progressStripes: undefined as string,
        tracks: {
            "Wait/ConcurrentlyRunningIndexesLimit": undefined as string,
            "Collection": undefined as string,
            "Indexing": undefined as string,
            "Cleanup": undefined as string,
            "References": undefined as string,
            "Map": undefined as string,
            "Storage/DocumentRead": undefined as string,
            "Linq": undefined as string,
            "Jint": undefined as string,
            "LoadDocument": undefined as string,
            "Bloom": undefined as string,
            "Lucene/Delete": undefined as string,
            "Lucene/Suggestion": undefined as string,
            "Lucene/AddDocument": undefined as string,
            "Lucene/Convert": undefined as string,
            "CreateBlittableJson": undefined as string,
            "Aggregation/BlittableJson": undefined as string,
            "GetMapEntriesTree": undefined as string,
            "GetMapEntries": undefined as string,
            "Storage/RemoveMapResult": undefined as string,
            "Storage/PutMapResult": undefined as string,
            "Reduce": undefined as string,
            "Tree": undefined as string,
            "Aggregation/Leafs": undefined as string,
            "Aggregation/Branches": undefined as string,
            "Storage/ReduceResults": undefined as string,
            "NestedValues": undefined as string,
            "Storage/Read": undefined as string,
            "Aggregation/NestedValues": undefined as string,
            "Lucene/Commit": undefined as string,
            "Lucene/ApplyDeletes": undefined as string,
            "Lucene/Merge": undefined as string,
            "Storage/Commit": undefined as string,
            "Lucene/RecreateSearcher": undefined as string,
            "SaveOutputDocuments": undefined as string,
            "DeleteOutputDocuments": undefined as string,
            "LoadCompareExchangeValue": undefined as string
        }
    };
    
    constructor() {
        super();
        
        this.bindToCurrentInstance("toggleScroll", "clearGraphWithConfirm");

        this.canExpandAll = ko.pureComputed(() => {
            const indexNames = this.indexNames();
            const expandedTracks = this.expandedTracks();

            return indexNames.length && indexNames.length !== expandedTracks.length;
        });

        this.searchText.throttle(200).subscribe(() => {
            this.filterIndexes();
            this.drawMainSection();
        });

        this.autoScroll.subscribe(v => {
            if (v) {
                this.scrollToRight();
            } else {
                // cancel transition (if any)
                this.brushContainer
                    .transition(); 
            }
        });

        this.loading = ko.pureComputed(() => {
            const client = this.liveViewClient();
            return client ? client.loading() : true;
        });
    }

    activate(args: { indexName: string, database: string}) {
        super.activate(args);

        if (args.indexName) {
            this.expandedTracks.push(args.indexName);
        }
        
        return new getIndexesStatsCommand(this.activeDatabase())
            .execute()
            .done((stats) => {
                this.faultyIndexes(stats.filter(x => x.Type === "Faulty").map(x => x.Name));
            });
    }

    deactivate() {
        super.deactivate();

        if (this.liveViewClient()) {
            this.cancelLiveView();
        }
    }

    compositionComplete() {
        super.compositionComplete();

        colorsManager.setup("#indexingPerformance", this.colors);
        
        this.scrollConfig = graphHelper.readScrollConfig();
        
        this.tooltip = d3.select(".tooltip");

        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalWidth -= 1;

        this.initCanvases();

        this.hitTest.init(this.svg,
            (indexName) => this.onToggleIndex(indexName),
            (item, x, y) => this.handleTrackTooltip(item, { x, y }),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();
    }
    
    private initCanvases() {
        const metricsContainer = d3.select("#indexPerfMetricsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight);

        this.inProgressCanvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight - indexPerformance.brushSectionHeight - indexPerformance.axisHeight)
            .style("top", (indexPerformance.brushSectionHeight + indexPerformance.axisHeight) + "px");

        const inProgressCanvasNode = this.inProgressCanvas.node() as HTMLCanvasElement;
        const inProgressContext = inProgressCanvasNode.getContext("2d");
        inProgressContext.translate(0, -indexPerformance.axisHeight);

        this.inProgressAnimator = new inProgressAnimator(inProgressCanvasNode);

        this.registerDisposable(this.inProgressAnimator);

        this.svg = metricsContainer
            .append("svg")
            .attr("width", this.totalWidth + 1)
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
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight - indexPerformance.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + indexPerformance.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        const onMove = () => {
            this.hitTest.onMouseMove();
        };

        this.hitTest.cursor.subscribe((cursor) => {
            selection.style('cursor', cursor);
        });

        selection.on("mousemove.tip", onMove);

        selection.on("click", () => this.hitTest.onClick());

        selection
            .on("mousedown.hit", () => {
                this.hitTest.onMouseDown();
                selection.on("mousemove.tip", null);
                if (this.liveViewClient()) {
                    this.liveViewClient().pauseUpdates();
                }
            })
            .on("mouseup.hit", () => {
                this.hitTest.onMouseUp();
                selection.on("mousemove.tip", onMove);
                if (this.liveViewClient()) {
                    this.liveViewClient().resumeUpdates();
                }
            });

        selection
            .on("mousedown.yShift", () => {
                const node = selection.node();
                const initialClickLocation = d3.mouse(node);
                const initialOffset = this.currentYOffset;

                selection.on("mousemove.yShift", () => {
                    const currentMouseLocation = d3.mouse(node);
                    const yDiff = currentMouseLocation[1] - initialClickLocation[1];

                    this.currentYOffset = initialOffset - yDiff;
                    this.fixCurrentOffset();
                });

                selection.on("mouseup.yShift", () => selection.on("mousemove.yShift", null));
            });

        selection.on("dblclick.zoom", null);
    }

    private filterIndexes() {
        const criteria = this.searchText().toLowerCase();

        this.filteredIndexNames(this.indexNames().filter(x => x.toLowerCase().includes(criteria)));
    }

    private enableLiveView() {
        let firstTime = true;

        const onDataUpdate = (data: Raven.Client.Documents.Indexes.IndexPerformanceStats[]) => {
            let timeRange: [Date, Date];
            if (!firstTime) {
                const timeToRemap = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
                timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
            }

            this.data = data;
            
            this.checkBufferUsage();

            const [workData, maxConcurrentItems] = this.prepareTimeData();

            if (!firstTime) {
                const newBrush = timeRange.map(x => this.xBrushTimeScale(x)) as [number, number];
                this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
            }

            if (this.autoScroll()) {
                this.scrollToRight();
            }

            this.draw(workData, maxConcurrentItems, firstTime);

            this.maybeUpdateTooltip();
            
            if (firstTime && this.data.length) {
                firstTime = false;
            }
        };

        this.liveViewClient(new liveIndexPerformanceWebSocketClient(this.activeDatabase(), onDataUpdate, this.dateCutoff));
    }
    
    private checkBufferUsage() {
        const dataCount = _.sumBy(this.data, x => x.Performance.length);
        const usage = Math.min(100, dataCount * 100.0 / indexPerformance.bufferSize);
        this.bufferUsage(usage.toFixed(1));
        
        if (dataCount > indexPerformance.bufferSize) {
            this.bufferIsFull(true);
            this.cancelLiveView();
        }
    }

    scrollToRight() {
        if (!this.hasAnyData()) {
            return;
        }
        
        const currentExtent = this.brush.extent() as [number, number];
        const extentWidth = currentExtent[1] - currentExtent[0];

        const existingBrushStart = currentExtent[0];

        if (currentExtent[1] < this.totalWidth) {

            const rightPadding = 100;
            const desiredShift = rightPadding * extentWidth / this.totalWidth;

            const desiredExtentStart = this.totalWidth + desiredShift - extentWidth;

            const moveFunc = (startX: number) => {
                this.brush.extent([startX, startX + extentWidth]);
                this.brushContainer.call(this.brush);

                this.onBrush();
            };

            this.brushContainer
                .transition()
                .duration(500)
                .tween("side-effect", () => {
                    const interpolator = d3.interpolate(existingBrushStart, desiredExtentStart);

                    return (t) => {
                        const currentStart = interpolator(t);
                        moveFunc(currentStart);
                    }
                });
        }
    }
    
    private maybeUpdateTooltip() {
        if (!this.currentTrackTooltipPosition) {
            return;
        }
        
        const datum = this.hitTest.findTrackItem(this.currentTrackTooltipPosition);
        
        this.handleTrackTooltip(datum, null, true);
    }

    private cancelLiveView() {
        if (!!this.liveViewClient()) {
            this.liveViewClient().dispose();
            this.liveViewClient(null);
        }
    }

    private draw(workData: workData[], maxConcurrentItems: number, resetFilteredIndexNames: boolean) {
        this.hasAnyData(this.data.length > 0);

        this.prepareBrushSection(workData, maxConcurrentItems);
        this.prepareMainSection(resetFilteredIndexNames);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.clearRect(0, 0, this.totalWidth, indexPerformance.brushSectionHeight);
        context.drawImage(this.brushSection, 0, 0);
        this.drawMainSection();
    }

    private prepareTimeData(): [workData[], number] {
        let timeRanges = this.extractTimeRanges(); 

        let maxConcurrentItems: number;
        let workData: workData[];

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
            maxConcurrentItems = 1;
            workData = [];
        } else {
            const aggregatedRanges = new rangeAggregator(timeRanges);
            workData = aggregatedRanges.aggregate();
            maxConcurrentItems = aggregatedRanges.maxConcurrentItems;
        }

        this.gapFinder = new gapFinder(timeRanges, indexPerformance.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        return [workData, maxConcurrentItems];
    }

    private prepareBrushSection(workData: workData[], maxConcurrentItems: number) {
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth + 1;
        this.brushSection.height = indexPerformance.brushSectionHeight;

        this.yBrushValueScale = d3.scale.linear()
            .domain([0, maxConcurrentItems])
            .range([0, indexPerformance.brushSectionIndexesWorkHeight]); 

        const context = this.brushSection.getContext("2d");

        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, indexPerformance.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = this.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth, indexPerformance.brushSectionHeight - 1);

        context.fillStyle = this.colors.brushChartColor;
        context.strokeStyle = this.colors.brushChartStrokeColor; 
        context.lineWidth = indexPerformance.brushSectionLineWidth;

        // Draw area chart showing indexes work
        let x1: number, x2: number, y0: number = 0, y1: number;
        context.beginPath();
        
        x2 = this.xBrushTimeScale(new Date(workData[0].pointInTime));
        
        for (let i = 0; i < workData.length - 1; i++) {
            x1 = x2;
            y1 = Math.round(this.yBrushValueScale(workData[i].numberOfItems)) + 0.5;
            x2 = this.xBrushTimeScale(new Date(workData[i + 1].pointInTime));
            context.moveTo(x1, indexPerformance.brushSectionHeight - y0);
            context.lineTo(x1, indexPerformance.brushSectionHeight - y1);

            // Don't want to draw line -or- rect at level 0
            if (y1 !== 0) {
                context.lineTo(x2, indexPerformance.brushSectionHeight - y1);
                context.fillRect(x1, indexPerformance.brushSectionHeight - y1, x2-x1, y1);
            } 

            y0 = y1; 
        }
        context.stroke();

        // Draw last line:
        context.beginPath();
        context.moveTo(x2, indexPerformance.brushSectionHeight - y1);
        context.lineTo(x2, indexPerformance.brushSectionHeight);
        context.stroke(); 

        this.drawBrushGaps(context);
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];

            context.strokeStyle = this.colors.gaps;

            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, indexPerformance.brushSectionHeight - 2);
            context.stroke();
        }
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
                .attr("height", indexPerformance.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilteredIndexNames: boolean) {
        this.findAndSetIndexNames();

        if (resetFilteredIndexNames) {
            this.searchText("");
        }
        this.filterIndexes();
    }

    private findAndSetIndexNames() {
        this.indexNames(_.uniq(this.data.map(x => x.Name)));
    }

    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {
        let currentOffset = indexPerformance.axisHeight - this.currentYOffset;
        const domain = [] as Array<string>;
        const range = [] as Array<number>;

        const indexesInfo = this.filteredIndexNames();

        for (let i = 0; i < indexesInfo.length; i++) {
            const indexName = indexesInfo[i];

            domain.push(indexName);
            range.push(currentOffset);

            const isOpened = _.includes(this.expandedTracks(), indexName);

            const itemHeight = isOpened ? indexPerformance.openedTrackHeight : indexPerformance.closedTrackHeight;

            currentOffset += itemHeight + indexPerformance.trackMargin;
        }

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {
        const expandedTracksCount = this.expandedTracks().length;
        const closedTracksCount = this.filteredIndexNames().length - expandedTracksCount;

        const offset = indexPerformance.axisHeight
            + this.filteredIndexNames().length * indexPerformance.trackMargin
            + expandedTracksCount * indexPerformance.openedTrackHeight
            + closedTracksCount * indexPerformance.closedTrackHeight;

        const availableHeightForTracks = this.totalHeight - indexPerformance.brushSectionHeight;

        const extraBottomMargin = 10;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private getTicks(scale: d3.time.Scale<number, number>) : Date[] {
        return d3.range(indexPerformance.initialOffset, this.totalWidth - indexPerformance.step, indexPerformance.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = this.colors.axis;
           
            ticks.forEach((x, i) => {
                context.moveTo(indexPerformance.initialOffset + (i * indexPerformance.step) + 0.5, yStart);
                context.lineTo(indexPerformance.initialOffset + (i * indexPerformance.step) + 0.5, yEnd);
            });

            context.stroke();
        }
        finally {
            context.restore();
        }
    }

    private drawXaxisTimeLabels(context: CanvasRenderingContext2D, ticks: Date[], timePaddingLeft: number, timePaddingTop: number) {
        try {
            context.save();
            context.beginPath();

            context.textAlign = "left";
            context.textBaseline = "top";
            context.font = "10px Lato";
            context.fillStyle = this.colors.axis;
           
            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), indexPerformance.initialOffset + (i * indexPerformance.step) + timePaddingLeft, timePaddingTop);
            });
        }
        finally {
            context.restore();
        }
    }

    toggleScroll() {
        this.autoScroll.toggle();
    }
    
    private onZoom() {
        this.autoScroll(false);
        this.clearSelectionVisible(true);

        if (!this.brushAndZoomCallbacksDisabled) {
            this.brush.extent(this.xNumericScale.domain() as [number, number]);
            this.brushContainer
                .call(this.brush);

            this.drawMainSection();
        }
    }

    private onBrush() {
        this.clearSelectionVisible(!this.brush.empty());

        if (!this.brushAndZoomCallbacksDisabled) {
            this.xNumericScale.domain((this.brush.empty() ? this.xBrushNumericScale.domain() : this.brush.extent()) as [number, number]);
            this.zoom.x(this.xNumericScale);
            this.drawMainSection();
        }
    }

    private extractTimeRanges(): Array<[Date, Date]> {
        const result = [] as Array<[Date, Date]>;
        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                const perfStatsWithCache = perfStat as IndexingPerformanceStatsWithCache;
                const start = perfStatsWithCache.StartedAsDateExcludingWaitTime;
                let end: Date;
                if (perfStat.Completed) {
                    end = perfStatsWithCache.CompletedAsDate;
                } else {
                    end = new Date(start.getTime() + perfStat.DurationInMs);
                }
                result.push([start, end]);
            });
        });

        return result;
    }

    private drawMainSection() {
        this.inProgressAnimator.reset();
        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];

        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.save();
        try {
            context.translate(0, indexPerformance.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);

            this.drawTracksBackground(context, xScale);

            if (xScale.domain().length) {
                const ticks = this.getTicks(xScale);

                context.save();
                context.beginPath();
                context.rect(0, indexPerformance.axisHeight - 3, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
                context.clip();
                const timeYStart = this.yScale.range()[0] || indexPerformance.axisHeight;
                this.drawXaxisTimeLines(context, ticks, timeYStart - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            context.save();
            try {
                context.beginPath();
                context.rect(0, indexPerformance.axisHeight, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
                context.clip();

                this.drawTracks(context, xScale, visibleTimeFrame);
                this.drawIndexNames(context);
                this.drawGaps(context, xScale);

                graphHelper.drawScroll(context,
                    { left: this.totalWidth, top: indexPerformance.axisHeight },
                    this.currentYOffset,
                    this.totalHeight - indexPerformance.brushSectionHeight - indexPerformance.axisHeight,
                    this.maxYOffset ? this.maxYOffset + this.totalHeight - indexPerformance.brushSectionHeight - indexPerformance.axisHeight: 0,
                    this.scrollConfig);

            } finally {
                context.restore();
            }
        } finally {
            context.restore();
        }

        this.inProgressAnimator.animate(this.colors.progressStripes);
    }

    private drawTracksBackground(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        context.save();

        context.beginPath();
        context.rect(0, indexPerformance.axisHeight, this.totalWidth, this.totalHeight - indexPerformance.brushSectionHeight);
        context.clip();

        this.data.forEach(perfStat => {
            const yStart = this.yScale(perfStat.Name);

            const isOpened = _.includes(this.expandedTracks(), perfStat.Name);

            context.beginPath();
            context.fillStyle = this.colors.trackBackground;
            context.fillRect(0, yStart, this.totalWidth, isOpened ? indexPerformance.openedTrackHeight : indexPerformance.closedTrackHeight);
        });

        context.restore();
    }

    private drawTracks(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>, visibleTimeFrame: [Date, Date]) {
        if (xScale.domain().length === 0) {
            return;
        }

        const visibleStartDateAsInt = visibleTimeFrame[0].getTime();
        const visibleEndDateAsInt = visibleTimeFrame[1].getTime();

        const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);

        this.data.forEach(perfStat => {
            if (!_.includes(this.filteredIndexNames(), perfStat.Name)) {
                return;
            }

            const isOpened = _.includes(this.expandedTracks(), perfStat.Name);
            let yStart = this.yScale(perfStat.Name);
            yStart += isOpened ? indexPerformance.openedTrackPadding : indexPerformance.closedTrackPadding;
            
            const trackEndY = yStart + (isOpened ? indexPerformance.openedTrackHeight : indexPerformance.closedTrackHeight);
            
            const trackVisibleOnScreen = trackEndY >= indexPerformance.axisHeight && yStart < this.totalHeight - indexPerformance.brushSectionHeight;

            const yOffset = isOpened ? indexPerformance.trackHeight + indexPerformance.stackPadding : 0;
            const stripesYStart = yStart + (isOpened ? yOffset : 0);
            
            if (trackVisibleOnScreen) {
                const performance = perfStat.Performance;
                const perfLength = performance.length;
                for (let perfIdx = 0; perfIdx < perfLength; perfIdx++) {
                    const perf = performance[perfIdx];
                    const perfWithCache = perf as IndexingPerformanceStatsWithCache;
                    const startDate = perfWithCache.StartedAsDate;

                    const startDateAsInt = startDate.getTime();
                    if (visibleEndDateAsInt < startDateAsInt) {
                        continue;
                    }

                    if (startDateAsInt + perf.DurationInMs < visibleStartDateAsInt)
                        continue;
                    
                    if (perfWithCache.WaitOperation && perfWithCache.WaitOperation.DurationInMs > 1) {
                        this.drawWaitTime(context, xScale(perfWithCache.StartedAsDate), stripesYStart, extentFunc, yOffset !== 0, perfWithCache.WaitOperation);
                    }
                    
                    const x1 = xScale(perfWithCache.StartedAsDateExcludingWaitTime);
                    this.drawStripes(0, perf, context, [perfWithCache.DetailsExcludingWaitTime], x1, stripesYStart, yOffset, extentFunc, perfStat.Name);

                    if (!perf.Completed) {
                        this.findInProgressAction(context, perfWithCache, extentFunc, x1, stripesYStart, yOffset);
                    }
                }
            }
        });
    }

    private findInProgressAction(context: CanvasRenderingContext2D, perf: IndexingPerformanceStatsWithCache, extentFunc: (duration: number) => number,
        xStart: number, yStart: number, yOffset: number): void {

        const extractor = (perfs: Raven.Client.Documents.Indexes.IndexingPerformanceOperation[], xStart: number, yStart: number, yOffset: number) => {

            let currentX = xStart;

            perfs.forEach(op => {
                const dx = extentFunc(op.DurationInMs);

                this.inProgressAnimator.register([currentX, yStart, dx, indexPerformance.trackHeight]);

                if (op.Operations.length > 0) {
                    extractor(op.Operations, currentX, yStart + yOffset, yOffset);
                }
                currentX += dx;
            });
        };

        extractor([perf.DetailsExcludingWaitTime], xStart, yStart, yOffset);
    }

    private getColorForOperation(operationName: string): string {
        const { tracks } = this.colors;
        if (operationName in tracks) {
            return (tracks as dictionary<string>)[operationName];
        }

        if (operationName.startsWith("Collection_")) {
            return tracks.Collection;
        }

        throw new Error("Unable to find color for: " + operationName);
    }

    
    private drawWaitTime(context: CanvasRenderingContext2D, xStart: number, yStart: number, extentFunc: (duration: number) => number, trackIsOpened: boolean, op: Raven.Client.Documents.Indexes.IndexingPerformanceOperation) {
        context.fillStyle = this.getColorForOperation("Wait/ConcurrentlyRunningIndexesLimit");
        const dx = extentFunc(op.DurationInMs);
        
        context.fillRect(xStart, yStart + indexPerformance.waitTrackPadding, dx, indexPerformance.trackHeight - 2 * indexPerformance.waitTrackPadding);
        
        if (trackIsOpened && dx >= 0.8) {
            this.hitTest.registerTrackItem(xStart, yStart + indexPerformance.waitTrackPadding, dx, indexPerformance.trackHeight - 2 * indexPerformance.waitTrackPadding, op);
        }
    }
    
    private drawStripes(level: number, rootPerf:Raven.Client.Documents.Indexes.IndexingPerformanceStats, 
                        context: CanvasRenderingContext2D, operations: Array<Raven.Client.Documents.Indexes.IndexingPerformanceOperation>, 
                        xStart: number, yStart: number, yOffset: number, extentFunc: (duration: number) => number, indexName?: string) {

        let currentX = xStart;
        const length = operations.length;
        for (let i = 0; i < length; i++) {
            const op = operations[i];
            context.fillStyle = this.getColorForOperation(op.Name);

            const dx = extentFunc(op.DurationInMs);

            context.fillRect(currentX, yStart, dx, indexPerformance.trackHeight);

            if (yOffset !== 0) { // track is opened
                if (dx >= 0.8) { // don't show tooltip for very small items
                    this.hitTest.registerTrackItem(currentX, yStart, dx, indexPerformance.trackHeight, op);
                }
       
                if (dx >= 5 && op.Name.startsWith("Collection_")) {
                    context.fillStyle = this.colors.stripeTextColor;
                    const text = op.Name.substr("Collection_".length);
                    const textWidth = context.measureText(text).width;
                    const truncatedText = graphHelper.truncText(text, textWidth, dx - 4);
                    if (truncatedText) {
                        context.font = "12px Lato";
                        context.fillText(truncatedText, currentX + 2, yStart + 13, dx - 4);
                    }
                } else if ((op.Name === "Map" || op.Name === "Reduce") && dx >= 6) {
                    context.fillStyle = this.colors.stripeTextColor;
                    const text = op.Name;
                    const textWidth = context.measureText(text).width;
                    const truncatedText = graphHelper.truncText(text, textWidth, dx - 4);
                    if (truncatedText) {
                        context.font = "12px Lato";
                        context.fillText(truncatedText, currentX + 2, yStart + 13, dx - 4);
                    }
                }
            } else { // track is closed
                if (indexName && dx >= 0.8) {
                    this.hitTest.registerIndexToggle(currentX, yStart, dx, indexPerformance.trackHeight, indexName);
                }
            }
            
            if ((level > 0 || dx > 1) && op.Operations.length > 0) {
                this.drawStripes(level + 1, rootPerf, context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc);
            }
            
            // check if item has errors - draw error marks *after* inner stripes to overlap
            if (level === 1) {
                if (op.Name === "Map" && rootPerf.FailedCount > 0) {
                    context.fillStyle = this.colors.itemWithError;
                    graphHelper.drawTriangle(context, currentX, yStart, dx);
                } else if (op.Name === "Reduce" && op.ReduceDetails && op.ReduceDetails.ReduceErrors > 0) {
                    context.fillStyle = this.colors.itemWithError;
                    graphHelper.drawTriangle(context, currentX, yStart, dx);
                }
            }
            
            currentX += dx;
        }
    }
    
    private drawIndexNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 14.5;
        const faultyWidth = 42;
        const textStart = 3 + 8 + 4;

        this.filteredIndexNames().forEach((indexName) => {
            context.font = "12px Lato";
            
            const isFaulty = !!this.faultyIndexes().find(x => x === indexName);
            
            const faultyExtraWidth = isFaulty ? faultyWidth : 0;
            
            const rectWidth = context.measureText(indexName).width + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4 /* padding between arrow and text */ + faultyExtraWidth; 

            context.fillStyle = this.colors.trackNameBg;
            context.fillRect(2, yScale(indexName) + indexPerformance.closedTrackPadding, rectWidth, indexPerformance.trackHeight);
            this.hitTest.registerIndexToggle(2, yScale(indexName), rectWidth, indexPerformance.trackHeight, indexName);
            context.fillStyle = this.colors.trackNameFg;
            context.fillText(indexName, textStart + 0.5, yScale(indexName) + textShift);

            const isOpened = _.includes(this.expandedTracks(), indexName);
            context.fillStyle = isOpened ? this.colors.openedTrackArrow : this.colors.closedTrackArrow;
            graphHelper.drawArrow(context, 5, yScale(indexName) + 6, !isOpened);
            
            if (isFaulty) {
                context.font = "12px Lato";
                context.fillStyle = this.colors.faulty;
                context.fillText("(Faulty)", rectWidth - faultyWidth, yScale(indexName) + textShift);
            }
        });
    }

    private drawGaps(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        // xScale.range has screen pixels locations of Activity periods
        // xScale.domain has Start & End times of Activity periods

        const range = xScale.range();

        context.beginPath();
        context.strokeStyle = this.colors.gaps;

        for (let i = 1; i < range.length - 1; i += 2) { 
            const gapX = Math.floor(range[i]) + 0.5;
            
            context.moveTo(gapX, indexPerformance.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                this.hitTest.registerGapItem(gapX - 5, indexPerformance.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }

    private onToggleIndex(indexName: string) {
        if (_.includes(this.expandedTracks(), indexName)) {
            this.expandedTracks.remove(indexName);
        } else {
            this.expandedTracks.push(indexName);
        }

        this.drawMainSection();
    }

    expandAll() {
        this.expandedTracks(this.indexNames().slice());
        this.drawMainSection();
    }

    collapseAll() {
        this.expandedTracks([]);
        this.drawMainSection();
    }
   
    private handleGapTooltip(element: timeGapInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = '<div class="tooltip-li">Gap start time: <div class="value">' + (element).start.toLocaleTimeString() + '</div></div>'
                + '<div class="tooltip-li">Gap duration: <div class="value">' + generalUtils.formatMillis((element).durationInMillis) + '</div></div>';
            this.handleTooltip(element, tooltipHtml, { x, y }, false);
        }
    } 

    private handleTrackTooltip(element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation, position: { x: number, y: number }, reuseTooltip: boolean = false) {
        if (!reuseTooltip) {
            this.currentTrackTooltipPosition = position;
        }
        
        const currentDatum = this.tooltip.datum();

        if (!element) {
            this.hideTooltip();
            return;
        }
        
        if (currentDatum !== element || reuseTooltip) {
            let tooltipHtml = `<div class="tooltip-header">${generalUtils.escapeHtml(element.Name)}</div> <div class="tooltip-li"> Duration: <div class="value">${generalUtils.formatMillis((element).DurationInMs)}</div></div>` ;

            const opWithParent = element as IndexingPerformanceOperationWithParent;

            if (opWithParent.Parent) {
                const parentStats = opWithParent.Parent;
                let countsDetails: string;
                countsDetails = `<div class="tooltip-header">Entries details</div>`;
                countsDetails += `<div class="tooltip-li">Input Count: <div class="value">${parentStats.InputCount.toLocaleString()}</div></div>`;
                countsDetails += `<div class="tooltip-li">Output Count: <div class="value">${parentStats.OutputCount.toLocaleString()}</div></div>`;
                countsDetails += `<div class="tooltip-li">Failed Count: <div class="value">${parentStats.FailedCount.toLocaleString()}</div></div>`;
                countsDetails += `<div class="tooltip-li">Success Count: <div class="value">${parentStats.SuccessCount.toLocaleString()}</div></div>`;
                countsDetails += `<div class="tooltip-li">Documents Size: <div class="value">${parentStats.DocumentsSize.HumaneSize}</div></div>`;

                if (parentStats.InputCount > 0) {
                    countsDetails += `<div class="tooltip-li">Average Document Size: <div class="value">${generalUtils.formatBytesToSize(parentStats.DocumentsSize.SizeInBytes / parentStats.InputCount)}</div></div>`;
                }

                countsDetails += `<div class="tooltip-li">Managed Allocation Size: <div class="value">${parentStats.AllocatedBytes.HumaneSize}</div></div>`;

                if (element.DurationInMs > 0) {
                    const durationInSec = element.DurationInMs / 1000;
                    countsDetails += `<div class="tooltip-li">Processed Data Speed: <div class="value">${generalUtils.formatBytesToSize(parentStats.DocumentsSize.SizeInBytes / durationInSec)}/sec</div></div>`;
                    countsDetails += `<div class="tooltip-li">Document Processing Speed: <div class="value">${Math.floor(parentStats.InputCount / durationInSec).toLocaleString()} docs/sec</div></div>`;
                }

                tooltipHtml += countsDetails;
            }

            if (element.CommitDetails) {
                let commitDetails: string;
                commitDetails = `<div class="tooltip-header">Commit details</div>`;
                commitDetails += `<div class="tooltip-li">Modified pages: <div class="value">${element.CommitDetails.NumberOfModifiedPages.toLocaleString()}</div></div>`;
                commitDetails += `<div class="tooltip-li">Pages written to disk: <div class="value">${element.CommitDetails.NumberOf4KbsWrittenToDisk.toLocaleString()}</div></div>`;
                tooltipHtml += commitDetails;
            }

            if (element.LuceneMergeDetails) {
                let luceneMergeDetails: string;
                luceneMergeDetails = `<div class="tooltip-header">Lucene Merge Details</div>`;
                luceneMergeDetails += `<div class="tooltip-li">Total merges: <div class="value">${element.LuceneMergeDetails.TotalMergesCount.toLocaleString()}</div></div>`;
                luceneMergeDetails += `<div class="tooltip-li">Executed merges: <div class="value">${element.LuceneMergeDetails.ExecutedMergesCount.toLocaleString()}</div></div>`;
                if (element.LuceneMergeDetails.MergedFilesCount > 0)
                    luceneMergeDetails += `<div class="tooltip-li">Merged files: <div class="value">${element.LuceneMergeDetails.MergedFilesCount.toLocaleString()}</div></div>`;
                if (element.LuceneMergeDetails.MergedDocumentsCount > 0)
                    luceneMergeDetails += `<div class="tooltip-li">Merged documents: <div class="value">${element.LuceneMergeDetails.MergedDocumentsCount.toLocaleString()}</div></div>`;
                tooltipHtml += luceneMergeDetails;
            }

            if (element.MapDetails) {
                let mapDetails: string;
                mapDetails = `<div class="tooltip-header">Map details</div>`;
                mapDetails += `<div class="tooltip-li">Allocation budget: <div class="value">${generalUtils.formatBytesToSize(element.MapDetails.AllocationBudget)}</div></div>`;
                mapDetails += `<div class="tooltip-li">Batch status: <div class="value">${element.MapDetails.BatchCompleteReason || 'In progress'}</div></div>`;
                mapDetails += `<div class="tooltip-li">Currently allocated: <div class="value">${generalUtils.formatBytesToSize(element.MapDetails.CurrentlyAllocated)} </div></div>`;
                mapDetails += `<div class="tooltip-li">Process private memory: <div class="value">${generalUtils.formatBytesToSize(element.MapDetails.ProcessPrivateMemory)}</div></div>`;
                mapDetails += `<div class="tooltip-li">Process working set: <div class="value">${generalUtils.formatBytesToSize(element.MapDetails.ProcessWorkingSet)}</div></div>`;
                tooltipHtml += mapDetails;
            }

            if (element.ReduceDetails) {
                let reduceDetails: string;

                if (element.ReduceDetails.TreesReduceDetails) {
                    reduceDetails = `<div class="tooltip-header">Trees details</div>`;
                    reduceDetails += `<div class="tooltip-li">Modified leafs: <div class="value">${element.ReduceDetails.TreesReduceDetails.NumberOfModifiedLeafs.toLocaleString()} (compressed: ${element.ReduceDetails.TreesReduceDetails.NumberOfCompressedLeafs.toLocaleString()})</div></div>`;
                    reduceDetails += `<div class="tooltip-li">Modified branches: <div class="value">${element.ReduceDetails.TreesReduceDetails.NumberOfModifiedBranches.toLocaleString()}</div></div>`;
                }
                else {
                    reduceDetails = `<div class="tooltip-header">Reduce details</div>`;
                    reduceDetails += `<div class="tooltip-li">Reduce attempts: <div class="value">${element.ReduceDetails.ReduceAttempts.toLocaleString()} </div></div>`;
                    reduceDetails += `<div class="tooltip-li">Reduce successes: <div class="value">${element.ReduceDetails.ReduceSuccesses.toLocaleString()} </div></div>`;
                    reduceDetails += `<div class="tooltip-li">Reduce errors: <div class="value">${element.ReduceDetails.ReduceErrors.toLocaleString()} </div></div>`;
                    reduceDetails += `<div class="tooltip-li">Currently allocated: <div class="value">${generalUtils.formatBytesToSize(element.ReduceDetails.CurrentlyAllocated)} </div></div>`;
                    reduceDetails += `<div class="tooltip-li">Process private memory: <div class="value">${generalUtils.formatBytesToSize(element.ReduceDetails.ProcessPrivateMemory)}</div></div>`;
                    reduceDetails += `<div class="tooltip-li">Process working set: <div class="value">${generalUtils.formatBytesToSize(element.ReduceDetails.ProcessWorkingSet)}</div></div>`;
                }
                
                tooltipHtml += reduceDetails;
            }           

            this.handleTooltip(element, tooltipHtml, position, reuseTooltip);
        }
    }
    
    private handleTooltip(element: Raven.Client.Documents.Indexes.IndexingPerformanceOperation | timeGapInfo, tooltipHtml: string, position: { x: number, y: number }, reuseTooltip: boolean) {
        if (element && (!this.dialogVisible || !position)) {

            this.tooltip
                .html(tooltipHtml)
                .datum(element)
                .style('display', undefined);
            
            const $tooltip = $(this.tooltip.node());
            const tooltipWidth = $tooltip.width();
            const tooltipHeight = $tooltip.height();

            if (!reuseTooltip) {
                let x = position.x;
                let y = position.y;
                x = Math.min(x, Math.max(this.totalWidth - tooltipWidth, 0));
                y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

                this.tooltip
                    .style("left", (x + 10) + "px")
                    .style("top", (y + 10) + "px");

                this.tooltip
                    .transition()
                    .duration(250)
                    .style("opacity", 1);
            }
        } else {
            this.hideTooltip();
        }
    }    

    private hideTooltip() {
        this.tooltip.transition()
            .duration(250)
            .style("opacity", 0)
            .each("end", () => this.tooltip.style("display", "none"));
         
        this.tooltip.datum(null);
    }

    fileSelected(fileInput: HTMLInputElement) {
        fileImporter.readAsText(fileInput, (data, fileName) => {
            this.dataImported(data);
            this.importFileName(fileName);
        });
    }

    private dataImported(result: string) {
        this.cancelLiveView();
        this.bufferIsFull(false);

        try {
            const importedData: Raven.Client.Documents.Indexes.IndexPerformanceStats[] = JSON.parse(result);

            // Data validation
            if (!_.isArray(importedData)) {
                messagePublisher.reportError("Invalid indexing performance file format", undefined, undefined);
            } else {
                this.data = importedData;
                this.fillCache();
                this.resetGraphData();
                const [workData, maxConcurrentItems] = this.prepareTimeData();
                this.draw(workData, maxConcurrentItems, true);
                this.isImport(true);
            }
        }
        catch (e) {
            messagePublisher.reportError("Failed to import indexing performance data", undefined, undefined);
        }
    }

    private fillCache() {
        this.data.forEach(indexStats => {
            indexStats.Performance.forEach(perfStat => {
                liveIndexPerformanceWebSocketClient.fillCache(perfStat);
            });
        });
    }

    clearGraphWithConfirm() {
        this.confirmationMessage("Clear graph data", "Do you want to discard all collected indexing performance information?")
            .done(result => {
                if (result.can) {
                    this.clearGraph();
                }
            })
    }
    
    clearGraph() {
        this.bufferIsFull(false);
        this.cancelLiveView();
        
        this.setCutOffDate();
        
        this.hasAnyData(false);
        this.resetGraphData();
        this.enableLiveView();
    }
    
    private setCutOffDate() {
        this.dateCutoff = d3.max(this.data, d => d3.max(d.Performance, (p: IndexingPerformanceStatsWithCache) => p.StartedAsDate));
    }
    
    closeImport() {
        this.dateCutoff = null;
        this.isImport(false);
        this.clearGraph();
    }

    private resetGraphData() {
        this.bufferUsage("0.0");
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());

        this.expandedTracks([]);
        this.searchText("");
    }

    private setZoomAndBrush(scale: [number, number], brushAction: (brush: d3.svg.Brush<any>) => void) {
        this.brushAndZoomCallbacksDisabled = true;

        this.xNumericScale.domain(scale);
        this.zoom.x(this.xNumericScale);

        brushAction(this.brush);
        
        if (this.brushContainer) {
            this.brushContainer.call(this.brush);
        }
        
        this.clearSelectionVisible(!this.brush.empty());
        this.brushAndZoomCallbacksDisabled = false;
    }

    exportAsJson() {
        let exportFileName: string;

        if (this.isImport()) {
            exportFileName = this.importFileName().substring(0, this.importFileName().lastIndexOf('.'));
        } else {
            exportFileName = `indexPerf of ${this.activeDatabase().name} ${moment().format("YYYY-MM-DD HH-mm")}`; 
        }

        const keysToIgnore: Array<keyof IndexingPerformanceStatsWithCache | keyof IndexingPerformanceOperationWithParent> = [
            "StartedAsDate", 
            "CompletedAsDate", 
            "Parent",
            "StartedAsDateExcludingWaitTime",
            "WaitOperation",
            "DetailsExcludingWaitTime"
        ];
        fileDownloader.downloadAsJson(this.data, exportFileName + ".json", exportFileName, (key, value) => {
            if (_.includes(keysToIgnore, key)) {
                return undefined;
            }
            return value;
        });
    }

    clearBrush() {
        this.autoScroll(false);
        this.brush.clear();
        this.brushContainer.call(this.brush);

        this.onBrush();
    }
}

export = indexPerformance; 
 
