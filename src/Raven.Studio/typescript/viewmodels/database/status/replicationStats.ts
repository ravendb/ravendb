import viewModelBase = require("viewmodels/viewModelBase");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");
import gapFinder = require("common/helpers/graph/gapFinder");
import generalUtils = require("common/generalUtils");
import rangeAggregator = require("common/helpers/graph/rangeAggregator");
import liveReplicationStatsWebSocketClient = require("common/liveReplicationStatsWebSocketClient");
import messagePublisher = require("common/messagePublisher");

import replication = Raven.Client.Documents.Replication;

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleReplication" | "trackItem" | "closedTrackItem" | "gapItem";
    arg: any;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleReplicationTrack: (replicationName: string) => void;
    private handleTrackTooltip: (item: Raven.Client.Documents.Replication.ReplicationPerformanceOperation, x: number, y: number) => void; 
    private handleClosedTrackTooltip: (item: ReplicationPerformanceBaseWithCache, x: number, y: number) => void;
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleReplication: (replicationName: string) => void,
        handleTrackTooltip: (item: Raven.Client.Documents.Replication.ReplicationPerformanceOperation, x: number, y: number) => void,
        handleClosedTrackTooltip: (item: ReplicationPerformanceBaseWithCache, x: number, y: number) => void,
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {
        this.container = container;
        this.onToggleReplicationTrack = onToggleReplication;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleClosedTrackTooltip = handleClosedTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Client.Documents.Replication.ReplicationPerformanceOperation) { //// !!! ???
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

    registerClosedTrackItem(x: number, y: number, width: number, height: number, element: ReplicationPerformanceBaseWithCache) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "closedTrackItem",
            arg: element
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerReplicationToggle(x: number, y: number, width: number, height: number, replicationName: string) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleReplication",
            arg: replicationName
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

            if (item.actionType === "toggleReplication") {
                this.onToggleReplicationTrack(item.arg as string);
                break;
            }
        }
    }

    onMouseDown() {
        this.cursor(graphHelper.prefixStyle("grabbing"));
    }

    onMouseUp() {
        this.cursor(graphHelper.prefixStyle("grab"));
    }

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const overToggleReplication = items.filter(x => x.actionType === "toggleReplication").length > 0;
        
        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Client.Documents.Replication.ReplicationPerformanceOperation)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
            this.cursor("auto");
        } else {
            const currentItem = items.filter(x => x.actionType === "closedTrackItem").map(x => x.arg as ReplicationPerformanceBaseWithCache)[0];
            if (currentItem) {
                this.handleClosedTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
                this.cursor("auto");
            } else {
                const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
                if (currentGapItem) {
                    this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
                    this.cursor("auto");
                } else {
                    this.removeTooltip();
                    this.cursor(overToggleReplication ? "pointer" : graphHelper.prefixStyle("grab"));
                }
            }
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - replicationStats.brushSectionHeight,
            maxY: y - replicationStats.brushSectionHeight
        });
    }
}

class replicationStats extends viewModelBase {

    /* static */

    static readonly colors = {
        axis: "#546175",
        gaps: "#ca1c59",
        brushChartColor: "#37404b",
        brushChartStrokeColor: "#008cc9",
        trackBackground: "#2c343a",
        separatorLine: "rgba(44, 52, 58, 0.65)",
        trackNameBg: "rgba(57, 67, 79, 0.95)",
        trackNameFg: "#98a7b7",
        trackDirectionText: "#baa50b",
        openedTrackArrow: "#ca1c59",
        closedTrackArrow: "#98a7b7",
        collectionNameTextColor: "#2c343a",
        itemWithError: "#98041b",

        tracks: {
            "Replication": "#0b4971",
            "Network/Read": "#046293",
            "Network/Write": "#046293",
            "Storage/Read": "#66418c",
            "Storage/Write": "#66418c", 
            "Network/DocumentRead": "#0077b5",
            "Network/AttachmentRead": "#008cc9",
            "Network/TombstoneRead": "#34b3e4",
            "Storage/DocumentRead": "#0077b5",
            "Storage/TombstoneRead": "#34b3e4",
            "Storage/AttachmentRead": "#008cc9",
            "Storage/CounterRead": "#27b5c9"
        }
    };

    static readonly brushSectionHeight = 40;
    private static readonly brushSectionReplicationWorkHeight = 22;
    private static readonly brushSectionLineWidth = 1;
    private static readonly trackHeight = 18; // height used for callstack item
    private static readonly stackPadding = 1; // space between call stacks
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly axisHeight = 35;
    private static readonly inProgressStripesPadding = 7;

    private static readonly maxRecursion = 5;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly bufferSize = 10000;


    private static readonly openedTrackHeight = replicationStats.openedTrackPadding
        + (replicationStats.maxRecursion + 1) * replicationStats.trackHeight
        + replicationStats.maxRecursion * replicationStats.stackPadding
        + replicationStats.openedTrackPadding;

    private static readonly closedTrackHeight = replicationStats.closedTrackPadding
        + replicationStats.trackHeight
        + replicationStats.closedTrackPadding;

    /* observables */

    hasAnyData = ko.observable<boolean>(false);
    loading: KnockoutComputed<boolean>;
    private searchText = ko.observable<string>();

    private liveViewClient = ko.observable<liveReplicationStatsWebSocketClient>();
    private autoScroll = ko.observable<boolean>(false);
    private clearSelectionVisible = ko.observable<boolean>(false); 
    
    private replicationTracksNames = ko.observableArray<string>();
    private filteredTrackNames = ko.observableArray<string>(); // the tracks to show - those that include the filter criteria..
    private expandedTracks = ko.observableArray<string>();
    private isImport = ko.observable<boolean>(false);
    private importFileName = ko.observable<string>();

    private canExpandAll: KnockoutComputed<boolean>;

    /* private */

    // The live data from endpoint
    private data: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[] = [];

    private bufferIsFull = ko.observable<boolean>(false);
    private bufferUsage = ko.observable<string>("0.0");
    private totalWidth: number;
    private totalHeight: number;
    private currentYOffset = 0;
    private maxYOffset = 0;
    private hitTest = new hitTest();
    private gapFinder: gapFinder;
    private dialogVisible = false;

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
    private tooltip: d3.Selection<Raven.Client.Documents.Replication.ReplicationPerformanceOperation | timeGapInfo | ReplicationPerformanceBaseWithCache>;

    constructor() {
        super();

        this.bindToCurrentInstance("clearGraphWithConfirm");
        
        this.canExpandAll = ko.pureComputed(() => {
            const replicationTracksNames = this.replicationTracksNames();
            const expandedTracks = this.expandedTracks();

            return replicationTracksNames.length && replicationTracksNames.length !== expandedTracks.length;
        });

        this.searchText.throttle(200).subscribe(() => {
            this.filterReplications();
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

    activate(args: { ReplicationName: string, database: string }): void {
        super.activate(args);

        if (args.ReplicationName) {
            this.expandedTracks.push(args.ReplicationName);
        }
    }

    deactivate() {
        super.deactivate();

        if (this.liveViewClient()) {
            this.cancelLiveView();
        }
    }

    compositionComplete() {
        super.compositionComplete();

        this.tooltip = d3.select(".tooltip");

        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalWidth -= 1;

        this.initCanvases();

        this.hitTest.init(this.svg,
            (replicationName) => this.onToggleReplication(replicationName),
            (item, x, y) => this.handleTrackTooltip(item, x, y),
            (closedTrackItem, x, y) => this.handleClosedTrackTooltip(closedTrackItem, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();
    }

    private initCanvases() {
        const metricsContainer = d3.select("#replicationStatsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight);

        this.inProgressCanvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight - replicationStats.brushSectionHeight - replicationStats.axisHeight) 
            .style("top", (replicationStats.brushSectionHeight + replicationStats.axisHeight) + "px");

        const inProgressCanvasNode = this.inProgressCanvas.node() as HTMLCanvasElement;
        const inProgressContext = inProgressCanvasNode.getContext("2d");
        inProgressContext.translate(0, -replicationStats.axisHeight);

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
            .attr("height", this.totalHeight - replicationStats.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + replicationStats.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        let mousePressed = false;

        const onMove = () => {
            this.hitTest.onMouseMove();
        }

        this.hitTest.cursor.subscribe((cursor) => {
            selection.style("cursor", cursor);
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
            });
        selection
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

                    const newYOffset = initialOffset - yDiff;

                    this.currentYOffset = newYOffset;
                    this.fixCurrentOffset();
                });

                selection.on("mouseup.yShift", () => selection.on("mousemove.yShift", null));
            });

        selection.on("dblclick.zoom", null);
    }

    private filterReplications() {
        this.filteredTrackNames(this.replicationTracksNames());

        const criteria = this.searchText().toLowerCase();
        if (criteria) {
            this.filteredTrackNames(this.replicationTracksNames().filter(x => x.toLowerCase().includes(criteria)));
        }
    }

    private enableLiveView() {
        let firstTime = true;

        const onDataUpdate = (data: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[]) => {
            let timeRange: [Date, Date];
            if (!firstTime) {
                const timeToRemap: [number,  number] = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
                timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
            }

            this.data = data;
            this.checkBufferUsage();

            const [workData, maxConcurrentReplications] = this.prepareTimeData();

            if (!firstTime) {
                const newBrush = timeRange.map(x => this.xBrushTimeScale(x)) as [number,  number];
                this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
            }

            if (this.autoScroll()) {
                this.scrollToRight();
            }

            this.draw(workData, maxConcurrentReplications, firstTime);

            if (firstTime) {
                firstTime = false;
            }
        };

        this.liveViewClient(new liveReplicationStatsWebSocketClient(this.activeDatabase(), onDataUpdate));
    }

    private checkBufferUsage() {
        const dataCount = _.sumBy(this.data, x => x.Performance.length);

        const usage = Math.min(100, dataCount * 100.0 / replicationStats.bufferSize);
        this.bufferUsage(usage.toFixed(1));

        if (dataCount > replicationStats.bufferSize) {
            this.bufferIsFull(true);
            this.cancelLiveView();
        }
    }

    scrollToRight() {
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
    
    toggleScroll() {
        this.autoScroll.toggle();
    }

    private cancelLiveView() {
        if (!!this.liveViewClient()) {
            this.liveViewClient().dispose();
            this.liveViewClient(null);
        }
    }

    private draw(workData: indexesWorkData[], maxConcurrentReplications: number, resetFilteredReplicationNames: boolean) {
        this.hasAnyData(this.data.length > 0);

        this.prepareBrushSection(workData, maxConcurrentReplications);
        this.prepareMainSection(resetFilteredReplicationNames);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.clearRect(0, 0, this.totalWidth, replicationStats.brushSectionHeight);
        context.drawImage(this.brushSection, 0, 0);
        this.drawMainSection();
    }

    private prepareTimeData(): [indexesWorkData[], number] {
        let timeRanges = this.extractTimeRanges();

        let maxConcurrentReplications: number;
        let workData: indexesWorkData[];

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
            maxConcurrentReplications = 1;
            workData = [];
        } else {
            const aggregatedRanges = new rangeAggregator(timeRanges);
            workData = aggregatedRanges.aggregate();
            maxConcurrentReplications = aggregatedRanges.maxConcurrentIndexes;
        }

        this.gapFinder = new gapFinder(timeRanges, replicationStats.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        return [workData, maxConcurrentReplications];
    }

    private prepareBrushSection(workData: indexesWorkData[], maxConcurrentReplications: number) {
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth + 1;
        this.brushSection.height = replicationStats.brushSectionHeight;

        this.yBrushValueScale = d3.scale.linear()
            .domain([0, maxConcurrentReplications])
            .range([0, replicationStats.brushSectionReplicationWorkHeight]);

        const context = this.brushSection.getContext("2d");

        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, replicationStats.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = replicationStats.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth, replicationStats.brushSectionHeight - 1);

        context.fillStyle = replicationStats.colors.brushChartColor;
        context.strokeStyle = replicationStats.colors.brushChartStrokeColor;
        context.lineWidth = replicationStats.brushSectionLineWidth;

        // Draw area chart showing replication work
        let x1: number, x2: number, y0: number = 0, y1: number;
        for (let i = 0; i < workData.length - 1; i++) {

            context.beginPath();
            x1 = this.xBrushTimeScale(new Date(workData[i].pointInTime));
            y1 = Math.round(this.yBrushValueScale(workData[i].numberOfIndexesWorking)) + 0.5;
            x2 = this.xBrushTimeScale(new Date(workData[i + 1].pointInTime));
            context.moveTo(x1, replicationStats.brushSectionHeight - y0);
            context.lineTo(x1, replicationStats.brushSectionHeight - y1);

            // Don't want to draw line -or- rect at level 0
            if (y1 !== 0) {
                context.lineTo(x2, replicationStats.brushSectionHeight - y1);
                context.fillRect(x1, replicationStats.brushSectionHeight - y1, x2 - x1, y1);
            }

            context.stroke();
            y0 = y1;
        }

        // Draw last line:
        context.beginPath();
        context.moveTo(x2, replicationStats.brushSectionHeight - y1);
        context.lineTo(x2, replicationStats.brushSectionHeight);
        context.stroke();

        this.drawBrushGaps(context);
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];

            context.strokeStyle = replicationStats.colors.gaps;

            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, replicationStats.brushSectionHeight - 2);
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
                .attr("height", replicationStats.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilteredReplicationsNames: boolean) {
        this.findAndSetReplicationsNames();

        if (resetFilteredReplicationsNames) {
            this.searchText("");
        }
        this.filterReplications();
    }

    private findAndSetReplicationsNames() {
        this.data = _.orderBy(this.data, [x => x.Type, x => x.Description], ["desc", "asc"]);
        this.replicationTracksNames(_.uniq(this.data.map(x => x.Description)));
    }

    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {
        let currentOffset = replicationStats.axisHeight - this.currentYOffset;
        const domain = [] as Array<string>;
        const range = [] as Array<number>;

        const replicationsInfo = this.filteredTrackNames();

        for (let i = 0; i < replicationsInfo.length; i++) {
            const replicationName = replicationsInfo[i];

            domain.push(replicationName);
            range.push(currentOffset);

            const isOpened = _.includes(this.expandedTracks(), replicationName);

            const itemHeight = isOpened ? replicationStats.openedTrackHeight : replicationStats.closedTrackHeight;

            currentOffset += itemHeight + replicationStats.trackMargin;
        }

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {
        const expandedTracksCount = this.expandedTracks().length;
        const closedTracksCount = this.filteredTrackNames().length - expandedTracksCount;

        const offset = replicationStats.axisHeight
            + this.filteredTrackNames().length * replicationStats.trackMargin
            + expandedTracksCount * replicationStats.openedTrackHeight
            + closedTracksCount * replicationStats.closedTrackHeight;

        const availableHeightForTracks = this.totalHeight - replicationStats.brushSectionHeight;

        const extraBottomMargin = 10;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private getTicks(scale: d3.time.Scale<number, number>): Date[] {
        
        return d3.range(replicationStats.initialOffset, this.totalWidth - replicationStats.step, replicationStats.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = replicationStats.colors.axis;

            ticks.forEach((x, i) => {
                context.moveTo(replicationStats.initialOffset + (i * replicationStats.step) + 0.5, yStart);
                context.lineTo(replicationStats.initialOffset + (i * replicationStats.step) + 0.5, yEnd);
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
            context.fillStyle = replicationStats.colors.axis;
          
            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), replicationStats.initialOffset + (i * replicationStats.step) + timePaddingLeft, timePaddingTop);
            });
        }
        finally {
            context.restore();
        }
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
        this.data.forEach(replicationStats => {
            replicationStats.Performance.forEach(perfStat => {
                const perfStatsWithCache = perfStat as ReplicationPerformanceBaseWithCache;
                const start = perfStatsWithCache.StartedAsDate;
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
            context.translate(0, replicationStats.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - replicationStats.brushSectionHeight);

            this.drawTracksBackground(context, xScale);

            if (xScale.domain().length) {

                const ticks = this.getTicks(xScale);

                context.save();
                context.beginPath();
                context.rect(0, replicationStats.axisHeight - 3, this.totalWidth, this.totalHeight - replicationStats.brushSectionHeight);
                context.clip();
                const timeYStart = this.yScale.range()[0] || replicationStats.axisHeight;
                this.drawXaxisTimeLines(context, ticks, timeYStart - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            context.save();
            try {
                context.beginPath();
                context.rect(0, replicationStats.axisHeight, this.totalWidth, this.totalHeight - replicationStats.brushSectionHeight);
                context.clip();

                this.drawTracks(context, xScale, visibleTimeFrame);
                this.drawReplicationTracksNames(context);
                this.drawGaps(context, xScale);

                graphHelper.drawScroll(context,
                    { left: this.totalWidth, top: replicationStats.axisHeight },
                    this.currentYOffset,
                    this.totalHeight - replicationStats.brushSectionHeight - replicationStats.axisHeight,
                    this.maxYOffset ? this.maxYOffset + this.totalHeight - replicationStats.brushSectionHeight - replicationStats.axisHeight : 0);

            } finally {
                context.restore();
            }
        } finally {
            context.restore();
        }
    }

    private drawTracksBackground(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        context.save();

        context.beginPath();
        context.rect(0, replicationStats.axisHeight, this.totalWidth, this.totalHeight - replicationStats.brushSectionHeight);
        context.clip();

        this.data.forEach(replicationStat => {
            const yStart = this.yScale(replicationStat.Description);
            const isOpened = _.includes(this.expandedTracks(), replicationStat.Description); 

            context.beginPath();
            context.fillStyle = replicationStats.colors.trackBackground;
            context.fillRect(0, yStart, this.totalWidth, isOpened ? replicationStats.openedTrackHeight : replicationStats.closedTrackHeight);
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

        this.data.forEach(replicationTrack => {
            if (!_.includes(this.filteredTrackNames(), replicationTrack.Description)) {
                return;
            }

            const isOpened = _.includes(this.expandedTracks(), replicationTrack.Description);
            let yStart = this.yScale(replicationTrack.Description);
            yStart += isOpened ? replicationStats.openedTrackPadding : replicationStats.closedTrackPadding;

            const performance = replicationTrack.Performance; 
            const perfLength = performance.length;
            let perfCompleted: string; 

            for (let perfIdx = 0; perfIdx < perfLength; perfIdx++) {
                const perf = performance[perfIdx];   // each performance[i] has:  completed, deteails, DurationInMilliseconds, id, started

                const perfWithCache = perf as ReplicationPerformanceBaseWithCache; // cache has also: startedAsDate, CompletedAsDate, Type
                const startDate = perfWithCache.StartedAsDate; 

                const x1 = xScale(startDate);
                const startDateAsInt = startDate.getTime();

                const endDateAsInt = startDateAsInt + perf.DurationInMs;
                if (endDateAsInt < visibleStartDateAsInt || visibleEndDateAsInt < startDateAsInt)
                    continue;

                const yOffset = isOpened ? replicationStats.trackHeight + replicationStats.stackPadding : 0;

                context.save();

                // 1. Draw perf items
                this.drawStripes(context, [perfWithCache.Details], x1, yStart + (isOpened ? yOffset : 0), yOffset, extentFunc, perfWithCache);

                // 2. Draw a separating line between adjacent perf items if needed
                if (perfIdx >= 1 && perfCompleted === perf.Started) {
                    context.fillStyle = replicationStats.colors.separatorLine;
                    context.fillRect(x1, yStart + (isOpened ? yOffset : 0), 1, replicationStats.trackHeight);
                }

                context.restore();

                // Save to compare with the start time of the next item...
                perfCompleted = perf.Completed; 
            }
        });
    }

    private getColorForOperation(operationName: string): string {
        const { tracks } = replicationStats.colors;

        if (operationName in tracks) {
            return (tracks as dictionary<string>)[operationName];
        }

        throw new Error("Unable to find color for: " + operationName);
    }

    private getType(replicationName: string): Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType {
        const replication = this.data.find(x => x.Description === replicationName);
        return replication.Type;
    }

    private drawStripes(context: CanvasRenderingContext2D, operations: Array<Raven.Client.Documents.Replication.ReplicationPerformanceOperation>,
        xStart: number, yStart: number, yOffset: number, extentFunc: (duration: number) => number,
        perfItemWithCache: ReplicationPerformanceBaseWithCache = null) {

        let currentX = xStart;
        const length = operations.length;
        for (let i = 0; i < length; i++) {
            const op = operations[i];
            const dx = extentFunc(op.DurationInMs);

            // 0. Draw item:
            context.fillStyle = this.getColorForOperation(op.Name);
            context.fillRect(currentX, yStart, dx, replicationStats.trackHeight);

            // Register items:
            // 1. Track is open
            if (yOffset !== 0) {
                if (dx >= 0.8) { // Don't show tooltip for very small items
                    if (op.Name !== "Replication") {
                        this.hitTest.registerTrackItem(currentX, yStart, dx, replicationStats.trackHeight, op);
                    } else if (perfItemWithCache) {
                        // Better to show full details for the first stripe.. 
                        this.hitTest.registerClosedTrackItem(currentX, yStart, dx, replicationStats.trackHeight, perfItemWithCache);
                    }
                }
            }
            // 2. Track is closed
            else if (perfItemWithCache) { 
                if (dx >= 0.8) { 
                    this.hitTest.registerClosedTrackItem(currentX, yStart, dx, replicationStats.trackHeight, perfItemWithCache);
                    this.hitTest.registerReplicationToggle(currentX, yStart, dx, replicationStats.trackHeight, perfItemWithCache.Description); 
                }
            }

            // 3. Draw inner/nested operations/stripes..
            if (op.Operations.length > 0) {
                this.drawStripes(context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc);
            }

            // 4. Handle errors if exist..(The very first 'replication' rect will be drawn on top of all others)
            if (perfItemWithCache) {
                if (perfItemWithCache.Errors) {
                    context.fillStyle = replicationStats.colors.itemWithError; 
                    context.fillRect(currentX, yStart, dx, replicationStats.trackHeight);
                }
            }

            currentX += dx;
        }
    }

    private drawReplicationTracksNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 14.5;
        const textStart = 3 + 8 + 4;

        this.filteredTrackNames().forEach((replicationName) => {
            context.font = "bold 12px Lato";
            const replicationType = this.getType(replicationName);

            const directionTextWidth = context.measureText(replicationType).width;
            let restOfText = (replicationType === "Outgoing") ? " to " : " from ";
            restOfText += replicationName;
            const restOfTextWidth = context.measureText(restOfText).width;

            const rectWidth = directionTextWidth + restOfTextWidth + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4; /* padding between arrow and text */

            context.fillStyle = replicationStats.colors.trackNameBg;
            context.fillRect(2, yScale(replicationName) + replicationStats.closedTrackPadding, rectWidth, replicationStats.trackHeight);
            this.hitTest.registerReplicationToggle(2, yScale(replicationName), rectWidth, replicationStats.trackHeight, replicationName);
            
            context.fillStyle = replicationStats.colors.trackDirectionText; 
            context.fillText(replicationType, textStart + 0.5, yScale(replicationName) + textShift);
            context.fillStyle = replicationStats.colors.trackNameFg;
            context.fillText(restOfText, textStart + directionTextWidth + 0.5, yScale(replicationName) + textShift);

            const isOpened = _.includes(this.expandedTracks(), replicationName);
            context.fillStyle = isOpened ? replicationStats.colors.openedTrackArrow : replicationStats.colors.closedTrackArrow;
            graphHelper.drawArrow(context, 5, yScale(replicationName) + 6, !isOpened);
        });
    }

    private drawGaps(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        // xScale.range has screen pixels locations of Activity periods
        // xScale.domain has Start & End times of Activity periods

        const range = xScale.range();

        context.beginPath();
        context.strokeStyle = replicationStats.colors.gaps;

        for (let i = 1; i < range.length - 1; i += 2) {
            const gapX = Math.floor(range[i]) + 0.5;

            context.moveTo(gapX, replicationStats.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                this.hitTest.registerGapItem(gapX - 5, replicationStats.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }

    private onToggleReplication(replicationName: string) {
        if (_.includes(this.expandedTracks(), replicationName)) {
            this.expandedTracks.remove(replicationName);
        } else {
            this.expandedTracks.push(replicationName);
        }

        this.drawMainSection();
    }

    expandAll() {
        this.expandedTracks(this.replicationTracksNames().slice());
        this.drawMainSection();
    }

    collapseAll() {
        this.expandedTracks([]);
        this.drawMainSection();
    }

    private handleGapTooltip(element: timeGapInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = "Gap start time: " + (element).start.toLocaleTimeString() +
                "<br/>Gap duration: " + generalUtils.formatMillis((element).durationInMillis);
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleClosedTrackTooltip(element: ReplicationPerformanceBaseWithCache, x: number, y: number) {
        const currentDatum = this.tooltip.datum();
        const baseElement = element as Raven.Client.Documents.Replication.ReplicationPerformanceBase;

        if (currentDatum !== element) {
            const duration = (element.DurationInMs === 0) ? "0" : generalUtils.formatMillis(element.DurationInMs);
            const direction = (element.Type === 'Outgoing') ? "Outgoing" : "Incoming";

            let tooltipHtml = `*** ${direction} Replication ***<br/>`;
            tooltipHtml += `Total duration: ${duration}<br/>`;

            switch (element.Type) {
                case "Incoming":
                    {
                        const elementWithData = baseElement as Raven.Client.Documents.Replication.IncomingReplicationPerformanceStats;
                        tooltipHtml += `Received last Etag: ${elementWithData.ReceivedLastEtag}<br/>`;
                        tooltipHtml += `Network input count: ${elementWithData.Network.InputCount}<br/>`; 
                        tooltipHtml += `Documents read count: ${elementWithData.Network.DocumentReadCount}<br/>`;
                        tooltipHtml += `Attachments read count: ${elementWithData.Network.AttachmentReadCount}<br/>`;
                    }
                    break;
                case "Outgoing":
                    {
                        const elementWithData = baseElement as Raven.Client.Documents.Replication.OutgoingReplicationPerformanceStats;
                        tooltipHtml += `Sent last Etag: ${elementWithData.SendLastEtag}<br/>`;
                        tooltipHtml += `Storage input count: ${elementWithData.Storage.InputCount}<br/>`;
                        tooltipHtml += `Documents output count: ${elementWithData.Network.DocumentOutputCount}<br/>`;
                        tooltipHtml += `Attachments read count: ${elementWithData.Network.AttachmentOutputCount}<br/>`;
                    }
                    break;
            }

            // Handle Errors:
            if (baseElement.Errors) {
                tooltipHtml += `<span style=color:Crimson;"><strong>Errors:</strong></span><br/>`;
                baseElement.Errors.forEach(err => tooltipHtml += `Errors: ${err.Error}<br/>`);
            }

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleTrackTooltip(element: Raven.Client.Documents.Replication.ReplicationPerformanceOperation,
                               x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            let tooltipHtml = `*** ${element.Name} ***<br/>`;
            tooltipHtml += `Duration: ${generalUtils.formatMillis((element).DurationInMs)}<br/>`;

            this.handleTooltip(element, x, y, tooltipHtml); 
        }
    }

    private handleTooltip(element: Raven.Client.Documents.Replication.ReplicationPerformanceOperation | timeGapInfo | ReplicationPerformanceBaseWithCache,
                          x: number, y: number, tooltipHtml: string) {
        if (element && !this.dialogVisible) {
            const canvas = this.canvas.node() as HTMLCanvasElement;
            const context = canvas.getContext("2d");
            context.font = this.tooltip.style("font");

            const longestLine = generalUtils.findLongestLine(tooltipHtml);
            const tooltipWidth = context.measureText(longestLine).width + 60;

            const numberOfLines = generalUtils.findNumberOfLines(tooltipHtml);
            const tooltipHeight = numberOfLines * 30 + 60;

            x = Math.min(x, Math.max(this.totalWidth - tooltipWidth, 0));
            y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

            this.tooltip
                .style("left", (x + 10) + "px")
                .style("top", (y + 10) + "px")
                .style('display', undefined);

            this.tooltip
                .transition()
                .duration(250)
                .style("opacity", 1);

            this.tooltip
                .html(tooltipHtml)
                .datum(element);
        } else {
            this.hideTooltip();
        }
    }

    private hideTooltip() {
        this.tooltip.transition()
            .duration(250)
            .style("opacity", 0);

        this.tooltip.datum(null);
    }

    fileSelected() {
        const fileInput = <HTMLInputElement>document.querySelector("#importFilePicker");
        const self = this;
        if (fileInput.files.length === 0) {
            return;
        }

        const file = fileInput.files[0];
        const reader = new FileReader();
        reader.onload = function () {
            self.dataImported(this.result);
        };
        reader.onerror = (error: any) => {
            alert(error);
        };
        reader.readAsText(file);

        this.importFileName(fileInput.files[0].name);

        // Must clear the filePicker element value so that user will be able to import the -same- file after closing the imported view...
        const $input = $("#importFilePicker");
        $input.val(null);
    }

    private dataImported(result: string) {
        this.cancelLiveView();
        this.bufferIsFull(false);

        try {
            const importedData: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[] = JSON.parse(result);

            // Data validation (currently only checking if this is an array, may do deeper validation later..
            if (!_.isArray(importedData)) { 
                messagePublisher.reportError("Invalid replication stats file format", undefined, undefined);
            } else {
                this.data = importedData;

                this.fillCache();
                this.prepareBrush(); 
                this.resetGraphData();
                const [workData, maxConcurrentReplications] = this.prepareTimeData();
                this.draw(workData, maxConcurrentReplications, true);

                this.isImport(true);
            }
        }
        catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
        }
    }

    private fillCache() {
        this.data.forEach(replicationStat => {
            replicationStat.Performance.forEach(perfStat => {
                liveReplicationStatsWebSocketClient.fillCache(perfStat, replicationStat.Type, replicationStat.Description);
            });
        });
    }

    clearGraphWithConfirm() {
        this.confirmationMessage("Clear graph data", "Do you want to discard all collected replication statistics?")
            .done(result => {
                if (result.can) {
                    this.clearGraph();
                }
            })
    }

    clearGraph() {
        this.bufferIsFull(false);
        this.cancelLiveView();
        this.hasAnyData(false);
        this.resetGraphData();
        this.enableLiveView();
    }

    closeImport() {
        this.isImport(false);
        this.clearGraph();
    }

    private resetGraphData() {
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());

        this.expandedTracks([]);
        this.searchText("");
    }

    private setZoomAndBrush(scale: [number, number], brushAction: (brush: d3.svg.Brush<any>) => void) {
        this.brushAndZoomCallbacksDisabled = true;

        this.xNumericScale.domain(scale);
        this.zoom.x(this.xNumericScale);

        brushAction(this.brush);
        this.brushContainer.call(this.brush);
        this.clearSelectionVisible(!this.brush.empty()); 

        this.brushAndZoomCallbacksDisabled = false;
    }

    exportAsJson() {
        let exportFileName: string;

        if (this.isImport()) {
            exportFileName = this.importFileName().substring(0, this.importFileName().lastIndexOf('.'));
        } else {
            exportFileName = `ReplicationStats of ${this.activeDatabase().name} ${moment().format("YYYY-MM-DD HH-mm")}`;
        }

        const keysToIgnore: Array<keyof ReplicationPerformanceBaseWithCache> = ["StartedAsDate", "CompletedAsDate"];
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

export = replicationStats;

