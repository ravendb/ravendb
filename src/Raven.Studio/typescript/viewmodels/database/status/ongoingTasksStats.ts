import viewModelBase = require("viewmodels/viewModelBase");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");
import gapFinder = require("common/helpers/graph/gapFinder");
import generalUtils = require("common/generalUtils");
import rangeAggregator = require("common/helpers/graph/rangeAggregator");
import liveReplicationStatsWebSocketClient = require("common/liveReplicationStatsWebSocketClient");
import liveEtlStatsWebSocketClient = require("common/liveEtlStatsWebSocketClient");
import messagePublisher = require("common/messagePublisher");
import inProgressAnimator = require("common/helpers/graph/inProgressAnimator");

import colorsManager = require("common/colorsManager");
import etlScriptDefinitionCache = require("models/database/stats/etlScriptDefinitionCache");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleTrack" | "trackItem" | "gapItem" | "previewScript";
    arg: any;
}

type taskOperation = Raven.Client.Documents.Replication.ReplicationPerformanceOperation | Raven.Server.Documents.ETL.Stats.EtlPerformanceOperation;
type performanceBaseWithCache = ReplicationPerformanceBaseWithCache | EtlPerformanceBaseWithCache;
type trackInfo = {
    name: string;
    type: ongoingTaskStatType;
    openedHeight: number;
    closedHeight: number;
}

type exportFileFormat = {
    Replication: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[];
    Etl: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats[];
}

type trackItemContext = {
    rootStats: performanceBaseWithCache;
    item: taskOperation;
}

type previewEtlScriptItemContext = {
    transformationName: string;
    taskId: number;
    etlType: Raven.Client.Documents.Operations.ETL.EtlType;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleTrack: (trackName: string) => void;
    private onPreviewScript: (context: previewEtlScriptItemContext) => void;
    private handleTrackTooltip: (context: trackItemContext, x: number, y: number) => void; 
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleTrack: (trackName: string) => void,
        onPreviewScript: (context: previewEtlScriptItemContext) => void,
        handleTrackTooltip: (context: trackItemContext, x: number, y: number) => void,
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {
        this.container = container;
        this.onToggleTrack = onToggleTrack;
        this.onPreviewScript = onPreviewScript;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, rootStats: performanceBaseWithCache, item: taskOperation) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "trackItem",
            arg: {
                rootStats, item
            } as trackItemContext
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerPreviewScript(x: number, y: number, width: number, height: number, taskInfo: previewEtlScriptItemContext) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "previewScript",
            arg: taskInfo
        } as rTreeLeaf;
        this.rTree.insert(data);
    }

    registerToggleTrack(x: number, y: number, width: number, height: number, trackName: string) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleTrack",
            arg: trackName
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

        const previewScript = items.find(x => x.actionType === "previewScript");
        if (previewScript) {
            this.onPreviewScript(previewScript.arg as previewEtlScriptItemContext);
        } else {
            const toggleTrack = items.find(x => x.actionType === "toggleTrack");
            if (toggleTrack) {
                this.onToggleTrack(toggleTrack.arg as string);
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

        const overToggleTrack = items.filter(x => x.actionType === "toggleTrack").length > 0;
        
        const currentPreviewItem = items.filter(x => x.actionType === "previewScript").map(x => x.arg as previewEtlScriptItemContext)[0];
        if (currentPreviewItem) {
            this.cursor("pointer");
        } else {
            const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as trackItemContext)[0];
            if (currentItem) {
                this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
                this.cursor("auto");
            } else {
                const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
                if (currentGapItem) {
                    this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
                    this.cursor("auto");
                } else {
                    this.removeTooltip();
                    this.cursor(overToggleTrack ? "pointer" : graphHelper.prefixStyle("grab"));
                }
            }
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - ongoingTasksStats.brushSectionHeight,
            maxY: y - ongoingTasksStats.brushSectionHeight
        });
    }
}

class ongoingTasksStats extends viewModelBase {

    /* static */
    static readonly brushSectionHeight = 40;
    private static readonly brushSectionTrackWorkHeight = 22;
    private static readonly brushSectionLineWidth = 1;
    private static readonly trackHeight = 18; // height used for callstack item
    private static readonly stackPadding = 1; // space between call stacks
    private static readonly trackMargin = 4;
    private static readonly betweenScriptsPadding = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly axisHeight = 35;
    private static readonly scriptNameLeftPadding = 14;
    private static readonly scriptPreviewIconWidth = 16;

    private static readonly maxReplicationRecursion = 3;
    private static readonly maxEtlRecursion = 2;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly bufferSize = 10000;

    private static readonly singleOpenedEtlItemHeight = ongoingTasksStats.maxEtlRecursion * ongoingTasksStats.trackHeight
        + (ongoingTasksStats.maxEtlRecursion - 1) * ongoingTasksStats.stackPadding;
    
    private static readonly openedReplicationTrackHeight = ongoingTasksStats.openedTrackPadding
        + (ongoingTasksStats.maxReplicationRecursion + 1) * ongoingTasksStats.trackHeight
        + ongoingTasksStats.maxReplicationRecursion * ongoingTasksStats.stackPadding
        + ongoingTasksStats.openedTrackPadding;

    private static readonly closedReplicationTrackHeight = ongoingTasksStats.closedTrackPadding
        + ongoingTasksStats.trackHeight
        + ongoingTasksStats.closedTrackPadding;

    /* observables */

    hasAnyData = ko.observable<boolean>(false);
    private firstDataChunkReceived = false;
    loading: KnockoutComputed<boolean>;
    private searchText = ko.observable<string>("");

    private liveViewReplicationClient = ko.observable<liveReplicationStatsWebSocketClient>();
    private liveViewEtlClient = ko.observable<liveEtlStatsWebSocketClient>();
    private autoScroll = ko.observable<boolean>(false);
    private clearSelectionVisible = ko.observable<boolean>(false); 
    
    private tracksInfo = ko.observableArray<trackInfo>();
    private filteredTrackNames = ko.observableArray<string>(); // the tracks to show - those that include the filter criteria..
    private expandedTracks = ko.observableArray<string>();
    private isImport = ko.observable<boolean>(false);
    private importFileName = ko.observable<string>();

    private canExpandAll: KnockoutComputed<boolean>;

    /* private */

    // The live data from endpoint
    private replicationData: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[] = [];
    private etlData: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats[] = [];
    
    private definitionsCache: etlScriptDefinitionCache;

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
    private updatesPaused = false;

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
    private tooltip: d3.Selection<taskOperation | timeGapInfo | performanceBaseWithCache>;

    /* colors */

    private scrollConfig: scrollColorConfig;
    private colors = { 
        axis: undefined as string,
        gaps: undefined as string,
        brushChartColor: undefined as string,
        brushChartStrokeColor: undefined as string,
        trackBackground: undefined as string,
        separatorLine: undefined as string,
        trackNameBg: undefined as string,
        trackNameFg: undefined as string,
        trackDirectionText: undefined as string,
        openedTrackArrow: undefined as string,
        closedTrackArrow: undefined as string,
        collectionNameTextColor: undefined as string,
        itemWithError: undefined as string,
        progressStripes: undefined as string,

        tracks: {
            "Replication": undefined as string,
            "Network/Read": undefined as string,
            "Network/Write": undefined as string,
            "Storage/Read": undefined as string,
            "Storage/Write": undefined as string,
            "Network/DocumentRead": undefined as string,
            "Network/AttachmentRead": undefined as string,
            "Network/TombstoneRead": undefined as string,
            "Storage/DocumentRead": undefined as string,
            "Storage/TombstoneRead": undefined as string,
            "Storage/AttachmentRead": undefined as string,
            "Storage/CounterRead": undefined as string,
            "ETL": undefined as string,
            "Extract": undefined as string,
            "Transform": undefined as string,
            "Load" : undefined as string
        }
    };
    
    constructor() {
        super();

        this.bindToCurrentInstance("clearGraphWithConfirm");
        
        this.canExpandAll = ko.pureComputed(() => {
            const tracksInfo = this.tracksInfo();
            const expandedTracks = this.expandedTracks();

            return tracksInfo.length && tracksInfo.length !== expandedTracks.length;
        });

        this.searchText.throttle(200).subscribe(() => {
            this.filterTracks();
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
            const replicationClient = this.liveViewReplicationClient();
            const etlClient = this.liveViewEtlClient();

            const replicationLoading = replicationClient ? replicationClient.loading() : true;
            const etlLoading = etlClient ? etlClient.loading() : true;
            
            return replicationLoading || etlLoading;
        });
    }

    activate(args: { TaskName: string, database: string }): void {
        super.activate(args);

        if (args.TaskName) {
            this.expandedTracks.push(args.TaskName);
        }
    }

    deactivate() {
        super.deactivate();

        if (this.liveViewReplicationClient() || this.liveViewEtlClient()) {
            this.cancelLiveView();
        }
    }

    compositionComplete() {
        super.compositionComplete();
        
        colorsManager.setup(".ongoing-tasks-stats", this.colors);
        this.scrollConfig = graphHelper.readScrollConfig();

        this.tooltip = d3.select(".tooltip");

        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalWidth -= 1;

        this.initCanvases();

        this.definitionsCache = new etlScriptDefinitionCache(this.activeDatabase());

        this.hitTest.init(this.svg,
            (replicationName) => this.onToggleTrack(replicationName),
            (context) => this.handlePreviewScript(context),
            (context, x, y) => this.handleTrackTooltip(context, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();
    }

    private initCanvases() {
        const metricsContainer = d3.select("#ongoingTasksStatsContainer"); 
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight);

        this.inProgressCanvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth + 1)
            .attr("height", this.totalHeight - ongoingTasksStats.brushSectionHeight - ongoingTasksStats.axisHeight) 
            .style("top", (ongoingTasksStats.brushSectionHeight + ongoingTasksStats.axisHeight) + "px");

        const inProgressCanvasNode = this.inProgressCanvas.node() as HTMLCanvasElement;
        const inProgressContext = inProgressCanvasNode.getContext("2d");
        inProgressContext.translate(0, -ongoingTasksStats.axisHeight);

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
            .attr("height", this.totalHeight - ongoingTasksStats.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + ongoingTasksStats.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
        const onMove = () => {
            this.hitTest.onMouseMove();
        };

        this.hitTest.cursor.subscribe((cursor) => {
            selection.style("cursor", cursor);
        });

        selection.on("mousemove.tip", onMove);

        selection.on("click", () => this.hitTest.onClick());

        selection
            .on("mousedown.hit", () => {
                this.hitTest.onMouseDown();
                selection.on("mousemove.tip", null);
                if (this.liveViewReplicationClient()) {
                    this.liveViewReplicationClient().pauseUpdates();
                }
                if (this.liveViewEtlClient()) {
                    this.liveViewEtlClient().pauseUpdates();
                }
                this.updatesPaused = true;
            });
        selection
            .on("mouseup.hit", () => {
                this.hitTest.onMouseUp();
                selection.on("mousemove.tip", onMove);
                if (this.liveViewReplicationClient()) {
                    this.liveViewReplicationClient().resumeUpdates();
                }
                if (this.liveViewEtlClient()) {
                    this.liveViewEtlClient().resumeUpdates();
                }
                this.updatesPaused = false;
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

    private filterTracks() {
        let tracks = this.tracksInfo().map(x => x.name);
        
        const criteria = this.searchText().toLowerCase();
        if (criteria) {
            tracks = tracks.filter(x => x.toLowerCase().includes(criteria));
        }

        this.filteredTrackNames(tracks);
    }

    private onDataUpdated() {
        let timeRange: [Date, Date];
        if (this.firstDataChunkReceived) {
            const timeToRemap: [number,  number] = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
            // noinspection JSSuspiciousNameCombination
            timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        }
        
        this.checkBufferUsage();

        const [workData, maxConcurrentActions] = this.prepareTimeData();

        if (this.firstDataChunkReceived) {
            const newBrush = timeRange.map(x => this.xBrushTimeScale(x)) as [number,  number];
            this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
        }

        if (this.autoScroll()) {
            this.scrollToRight();
        }

        this.draw(workData, maxConcurrentActions, !this.firstDataChunkReceived);

        if (!this.firstDataChunkReceived) {
            this.firstDataChunkReceived = true;
        }
    }
    
    private enableLiveView() {
        this.firstDataChunkReceived = false;
        
        // since we are fetching data from 2 different sources
        // let's throttle updates to avoid jumpy UI
        const onDataUpdatedThrottle = _.debounce(() => {
            if (!this.updatesPaused) {
                this.onDataUpdated(); 
            }
        }, 1000, { maxWait: 3000 });

        this.liveViewReplicationClient(new liveReplicationStatsWebSocketClient(this.activeDatabase(), d => {
            this.replicationData = d;
            onDataUpdatedThrottle();
        }, this.dateCutoff));
        this.liveViewEtlClient(new liveEtlStatsWebSocketClient(this.activeDatabase(), d => {
            this.etlData = d;
            onDataUpdatedThrottle();
        }, this.dateCutoff));
    }

    private checkBufferUsage() {
        const replicationDataCount = _.sumBy(this.replicationData, x => x.Performance.length);
        const etlDataCount = _.sumBy(this.etlData, t => _.sumBy(t.Stats, s => s.Performance.length));
        
        const dataCount = replicationDataCount + etlDataCount;

        const usage = Math.min(100, dataCount * 100.0 / ongoingTasksStats.bufferSize);
        this.bufferUsage(usage.toFixed(1));

        if (dataCount > ongoingTasksStats.bufferSize) {
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
        if (!!this.liveViewReplicationClient()) {
            this.liveViewReplicationClient().dispose();
            this.liveViewReplicationClient(null);
        }

        if (!!this.liveViewEtlClient()) {
            this.liveViewEtlClient().dispose();
            this.liveViewEtlClient(null);
        }
    }

    private draw(workData: workData[], maxConcurrentActions: number, resetFilter: boolean) {
        this.hasAnyData(this.replicationData.length > 0 || this.etlData.length > 0);

        this.prepareBrushSection(workData, maxConcurrentActions);
        this.prepareMainSection(resetFilter);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.clearRect(0, 0, this.totalWidth, ongoingTasksStats.brushSectionHeight);
        context.drawImage(this.brushSection, 0, 0);
        this.drawMainSection();
    }

    private prepareTimeData(): [workData[], number] {
        let timeRanges = this.extractTimeRanges();

        let maxConcurrentActions: number;
        let workData: workData[];

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
            maxConcurrentActions = 1;
            workData = [];
        } else {
            const aggregatedRanges = new rangeAggregator(timeRanges);
            workData = aggregatedRanges.aggregate();
            maxConcurrentActions = aggregatedRanges.maxConcurrentItems;
        }

        this.gapFinder = new gapFinder(timeRanges, ongoingTasksStats.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        return [workData, maxConcurrentActions];
    }

    private prepareBrushSection(workData: workData[], maxConcurrentActions: number) {
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth + 1;
        this.brushSection.height = ongoingTasksStats.brushSectionHeight;

        this.yBrushValueScale = d3.scale.linear()
            .domain([0, maxConcurrentActions])
            .range([0, ongoingTasksStats.brushSectionTrackWorkHeight]);

        const context = this.brushSection.getContext("2d");

        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, ongoingTasksStats.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = this.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth, ongoingTasksStats.brushSectionHeight - 1);

        context.fillStyle = this.colors.brushChartColor;
        context.strokeStyle = this.colors.brushChartStrokeColor;
        context.lineWidth = ongoingTasksStats.brushSectionLineWidth;

        // Draw area chart showing replication work
        let x1: number, x2: number, y0: number = 0, y1: number;
        for (let i = 0; i < workData.length - 1; i++) {

            context.beginPath();
            x1 = this.xBrushTimeScale(new Date(workData[i].pointInTime));
            y1 = Math.round(this.yBrushValueScale(workData[i].numberOfItems)) + 0.5;
            x2 = this.xBrushTimeScale(new Date(workData[i + 1].pointInTime));
            context.moveTo(x1, ongoingTasksStats.brushSectionHeight - y0);
            context.lineTo(x1, ongoingTasksStats.brushSectionHeight - y1);

            // Don't want to draw line -or- rect at level 0
            if (y1 !== 0) {
                context.lineTo(x2, ongoingTasksStats.brushSectionHeight - y1);
                context.fillRect(x1, ongoingTasksStats.brushSectionHeight - y1, x2 - x1, y1);
            }

            context.stroke();
            y0 = y1;
        }

        // Draw last line:
        context.beginPath();
        context.moveTo(x2, ongoingTasksStats.brushSectionHeight - y1);
        context.lineTo(x2, ongoingTasksStats.brushSectionHeight);
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
            context.lineTo(gapX, ongoingTasksStats.brushSectionHeight - 2);
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
                .attr("height", ongoingTasksStats.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilter: boolean) {
        this.findAndSetTaskNames();

        if (resetFilter) {
            this.searchText("");
        }
        this.filterTracks();
    }

    private findAndSetTaskNames() {
        this.replicationData = _.orderBy(this.replicationData, [x => x.Type, x => x.Description], ["desc", "asc"]);
        this.etlData = _.orderBy(this.etlData, [x => x.EtlType, x => x.TaskName], ["asc", "asc"]);
        
        this.etlData.forEach(etl => {
            etl.Stats = _.orderBy(etl.Stats, [x => x.TransformationName], ["asc"]);
        });
        
        const trackInfos = [] as trackInfo[];
        
        this.replicationData.forEach(replication => {
            trackInfos.push({
                name: replication.Description,
                type: replication.Type,
                openedHeight: ongoingTasksStats.openedReplicationTrackHeight,
                closedHeight: ongoingTasksStats.closedReplicationTrackHeight
            })
        });
        
        this.etlData.forEach(taskInfo => {
            const scriptsCount = taskInfo.Stats.length;

            const closedHeight = ongoingTasksStats.openedTrackPadding
                + (scriptsCount + 1) * ongoingTasksStats.trackHeight
                + scriptsCount * ongoingTasksStats.betweenScriptsPadding
                + ongoingTasksStats.openedTrackPadding;
            
            const openedHeight = 2 * ongoingTasksStats.openedTrackPadding
                + ongoingTasksStats.trackHeight
                + (scriptsCount - 1) * ongoingTasksStats.betweenScriptsPadding
                + scriptsCount * ongoingTasksStats.singleOpenedEtlItemHeight
                + ongoingTasksStats.openedTrackPadding;
            
            trackInfos.push({
                name: taskInfo.TaskName,
                type: taskInfo.EtlType,
                openedHeight: openedHeight, 
                closedHeight: closedHeight
            });
        });
        
        this.tracksInfo(trackInfos);
    }
    
    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {
        let currentOffset = ongoingTasksStats.axisHeight - this.currentYOffset;
        const domain = [] as Array<string>;
        const range = [] as Array<number>;

        const trackNames = this.filteredTrackNames();

        for (let i = 0; i < trackNames.length; i++) {
            const trackName = trackNames[i];

            domain.push(trackName);
            range.push(currentOffset);

            const isOpened = _.includes(this.expandedTracks(), trackName);

            const trackInfo = this.tracksInfo().find(x => x.name === trackName);

            currentOffset += (isOpened ? trackInfo.openedHeight : trackInfo.closedHeight) + ongoingTasksStats.trackMargin;
        }

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }
    
    private calcMaxYOffset() {
        const heightSum = _.sumBy(this.filteredTrackNames(), track => {
            const isOpened = _.includes(this.expandedTracks(), track);
            const trackInfo = this.tracksInfo().find(x => x.name === track);
            return isOpened ? trackInfo.openedHeight : trackInfo.closedHeight;
        });
        
        const offset = ongoingTasksStats.axisHeight
            + this.filteredTrackNames().length * ongoingTasksStats.trackMargin
            + heightSum;

        const availableHeightForTracks = this.totalHeight - ongoingTasksStats.brushSectionHeight;

        const extraBottomMargin = 10;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private getTicks(scale: d3.time.Scale<number, number>): Date[] {
        return d3.range(ongoingTasksStats.initialOffset, this.totalWidth - ongoingTasksStats.step, ongoingTasksStats.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = this.colors.axis;

            ticks.forEach((x, i) => {
                context.moveTo(ongoingTasksStats.initialOffset + (i * ongoingTasksStats.step) + 0.5, yStart);
                context.lineTo(ongoingTasksStats.initialOffset + (i * ongoingTasksStats.step) + 0.5, yEnd);
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
                context.fillText(this.xTickFormat(x), ongoingTasksStats.initialOffset + (i * ongoingTasksStats.step) + timePaddingLeft, timePaddingTop);
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
        
        const onPerf = (perfStatsWithCache: performanceBaseWithCache) => {
            const start = perfStatsWithCache.StartedAsDate;
            let end: Date;
            if (perfStatsWithCache.Completed) {
                end = perfStatsWithCache.CompletedAsDate;
            } else {
                end = new Date(start.getTime() + perfStatsWithCache.DurationInMs);
            }
            result.push([start, end]);
        };
        
        this.replicationData.forEach(replicationStats => {
            replicationStats.Performance.forEach(perfStat => onPerf(perfStat as performanceBaseWithCache));
        });
        
        this.etlData.forEach(etlStats => {
            etlStats.Stats.forEach(etlStat => {
                etlStat.Performance.forEach(perfStat => onPerf(perfStat as performanceBaseWithCache));
            })
        });

        return result;
    }

    private drawMainSection() {
        this.inProgressAnimator.reset();
        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        // noinspection JSSuspiciousNameCombination
        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.save();
        try {
            context.translate(0, ongoingTasksStats.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - ongoingTasksStats.brushSectionHeight);

            this.drawTracksBackground(context);

            if (xScale.domain().length) {

                const ticks = this.getTicks(xScale);

                context.save();
                context.beginPath();
                context.rect(0, ongoingTasksStats.axisHeight - 3, this.totalWidth, this.totalHeight - ongoingTasksStats.brushSectionHeight);
                context.clip();
                const timeYStart = this.yScale.range()[0] || ongoingTasksStats.axisHeight;
                this.drawXaxisTimeLines(context, ticks, timeYStart - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            context.save();
            try {
                context.beginPath();
                context.rect(0, ongoingTasksStats.axisHeight, this.totalWidth, this.totalHeight - ongoingTasksStats.brushSectionHeight);
                context.clip();

                this.drawTracks(context, xScale, visibleTimeFrame);
                this.drawTracksNames(context);
                this.drawGaps(context, xScale);

                graphHelper.drawScroll(context,
                    { left: this.totalWidth, top: ongoingTasksStats.axisHeight },
                    this.currentYOffset,
                    this.totalHeight - ongoingTasksStats.brushSectionHeight - ongoingTasksStats.axisHeight,
                    this.maxYOffset ? this.maxYOffset + this.totalHeight - ongoingTasksStats.brushSectionHeight - ongoingTasksStats.axisHeight : 0, 
                    this.scrollConfig);

            } finally {
                context.restore();
            }
        } finally {
            context.restore();
        }
        
        this.inProgressAnimator.animate(this.colors.progressStripes);
    }

    private drawTracksBackground(context: CanvasRenderingContext2D) {
        context.save();

        context.beginPath();
        context.rect(0, ongoingTasksStats.axisHeight, this.totalWidth, this.totalHeight - ongoingTasksStats.brushSectionHeight);
        context.clip();

        const drawBackground = (trackName: string) => {
            const yStart = this.yScale(trackName);
            const isOpened = _.includes(this.expandedTracks(), trackName);
            const trackInfo = this.tracksInfo().find(x => x.name === trackName);

            context.beginPath();
            context.fillStyle = this.colors.trackBackground;
            context.fillRect(0, yStart, this.totalWidth, isOpened ? trackInfo.openedHeight : trackInfo.closedHeight);
        };
        
        this.replicationData.forEach(replicationStat => {
           drawBackground(replicationStat.Description);
        });
        this.etlData.forEach(x => {
            drawBackground(x.TaskName);
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

        const drawTrack = (trackName: string, yStart: number, isOpened: boolean, performance: performanceBaseWithCache[]) => {
            yStart += isOpened ? ongoingTasksStats.openedTrackPadding : ongoingTasksStats.closedTrackPadding;

            const perfLength = performance.length;
            let perfCompleted: string = null;

            for (let perfIdx = 0; perfIdx < perfLength; perfIdx++) {
                const perf = performance[perfIdx];   // each performance[i] has:  completed, details, DurationInMilliseconds, id, started

                const perfWithCache = perf as performanceBaseWithCache; // cache has also: startedAsDate, CompletedAsDate, Type
                const startDate = perfWithCache.StartedAsDate;

                const x1 = xScale(startDate);
                const startDateAsInt = startDate.getTime();

                const endDateAsInt = startDateAsInt + perf.DurationInMs;
                if (endDateAsInt < visibleStartDateAsInt || visibleEndDateAsInt < startDateAsInt)
                    continue;

                const yOffset = isOpened ? ongoingTasksStats.trackHeight + ongoingTasksStats.stackPadding : 0;
                const stripesYStart = yStart + (isOpened ? yOffset : 0);

                context.save();

                // 1. Draw perf items
                this.drawStripes(context, [perfWithCache.Details], x1, stripesYStart, yOffset, extentFunc, perfWithCache, trackName);

                // 2. Draw a separating line between adjacent perf items if needed
                if (perfIdx >= 1 && perfCompleted === perf.Started) {
                    context.fillStyle = this.colors.separatorLine;
                    context.fillRect(x1, yStart + (isOpened ? yOffset : 0), 1, ongoingTasksStats.trackHeight);
                }

                context.restore();

                // Save to compare with the start time of the next item...
                perfCompleted = perf.Completed;

                if (!perf.Completed) {
                    this.findInProgressAction(perf, extentFunc, x1, stripesYStart, yOffset);
                }
            }
        };
        
        this.replicationData.forEach(replicationTrack => {
            const trackName = replicationTrack.Description;
            if (_.includes(this.filteredTrackNames(), trackName)) {
                const isOpened = _.includes(this.expandedTracks(), trackName);
                drawTrack(trackName, this.yScale(trackName), isOpened, replicationTrack.Performance as performanceBaseWithCache[]);
            }
            
        });
        
        this.etlData.forEach(etlItem => {
            const trackName = etlItem.TaskName;
            if (_.includes(this.filteredTrackNames(), trackName)) {
                const yStartBase = this.yScale(trackName);
                const isOpened = _.includes(this.expandedTracks(), trackName);
                const extraPadding = isOpened ? ongoingTasksStats.trackHeight + ongoingTasksStats.stackPadding + ongoingTasksStats.openedTrackPadding 
                    : ongoingTasksStats.closedTrackPadding;
                
                etlItem.Stats.forEach((etlStat, idx) => {
                    context.font = "10px Lato";
                    const openedTrackItemOffset = ongoingTasksStats.betweenScriptsPadding + ongoingTasksStats.singleOpenedEtlItemHeight;
                    const closedTrackItemOffset = ongoingTasksStats.betweenScriptsPadding + ongoingTasksStats.trackHeight;
                    const offset = isOpened ? idx * openedTrackItemOffset : (idx + 1) * closedTrackItemOffset;
                    drawTrack(trackName, yStartBase + offset, isOpened, etlStat.Performance as performanceBaseWithCache[]);
                    this.drawScriptName(context, yStartBase + offset + extraPadding, {
                        transformationName: etlStat.TransformationName,
                        etlType: etlItem.EtlType,
                        taskId: etlItem.TaskId
                    });
                });
            }
        })
    }
    
    private drawScriptName(context: CanvasRenderingContext2D, yStart: number, taskInfo: previewEtlScriptItemContext) {
        const textShift = 12.5;
        context.font = "bold 12px Lato";
        const transformationNameWidth = context.measureText(taskInfo.transformationName).width + 8;

        const areaWidth = transformationNameWidth + ongoingTasksStats.scriptNameLeftPadding * 2 + ongoingTasksStats.scriptPreviewIconWidth;
        
        context.fillStyle = this.colors.trackNameBg;
        context.fillRect(2, yStart, areaWidth, ongoingTasksStats.trackHeight);

        context.fillStyle = this.colors.trackNameFg;
        context.fillText(taskInfo.transformationName, ongoingTasksStats.scriptNameLeftPadding + 4, yStart + textShift);
        
        context.font = "16px icomoon";
        context.fillText('\ue9a3',ongoingTasksStats.scriptNameLeftPadding + transformationNameWidth + ongoingTasksStats.scriptPreviewIconWidth / 2, yStart + 16);
        
        this.hitTest.registerPreviewScript(2, yStart, areaWidth, ongoingTasksStats.trackHeight, taskInfo);
    }

    private findInProgressAction(perf: performanceBaseWithCache, extentFunc: (duration: number) => number,
                                 xStart: number, yStart: number, yOffset: number): void {

        const extractor = (perfs: taskOperation[], xStart: number, yStart: number, yOffset: number) => {

            let currentX = xStart;

            perfs.forEach(op => {
                const dx = extentFunc(op.DurationInMs);

                this.inProgressAnimator.register([currentX, yStart, dx, ongoingTasksStats.trackHeight]);

                if (op.Operations.length > 0) {
                    extractor(op.Operations, currentX, yStart + yOffset, yOffset);
                }
                currentX += dx;
            });
        };

        extractor([perf.Details], xStart, yStart, yOffset);
    }

    private getColorForOperation(operationName: string): string {
        const { tracks } = this.colors;

        if (operationName in tracks) {
            return (tracks as dictionary<string>)[operationName];
        }

        throw new Error("Unable to find color for: " + operationName);
    }

    private getTaskType(taskName: string): ongoingTaskStatType {
        return this.tracksInfo().find(x => x.name === taskName).type;
    }
    
    private getTaskTypeDescription(type: ongoingTaskStatType): string {
        switch (type) {
            case "IncomingPush":
                return "Incoming External Replication";
            case "IncomingPull":
                return "Incoming Pull Replication";
            case "OutgoingPush":
                return "Outgoing External Replication";
            case "OutgoingPull":
                return "Outgoing Pull Replication";
            case "Raven":
                return "Raven ETL";
            case "Sql":
                return "SQL ETL";
        }
        return "";
    }

    private drawStripes(context: CanvasRenderingContext2D, operations: Array<taskOperation>,
        xStart: number, yStart: number, yOffset: number, extentFunc: (duration: number) => number,
        perfItemWithCache: performanceBaseWithCache, trackName: string) {

        let currentX = xStart;
        const length = operations.length;
        for (let i = 0; i < length; i++) {
            const op = operations[i];
            const dx = extentFunc(op.DurationInMs);
            const isRootOperation = perfItemWithCache.Details === op;

            // 0. Draw item:
            context.fillStyle = this.getColorForOperation(op.Name);
            context.fillRect(currentX, yStart, dx, ongoingTasksStats.trackHeight);

            // Register items:
            // 1. Track is open
            if (yOffset !== 0) {
                if (dx >= 0.8) { // Don't show tooltip for very small items
                    this.hitTest.registerTrackItem(currentX, yStart, dx, ongoingTasksStats.trackHeight, perfItemWithCache, op);
                }
            }
            // 2. Track is closed
            else if (isRootOperation) { // register only on root item
                if (dx >= 0.8) {
                    this.hitTest.registerTrackItem(currentX, yStart, dx, ongoingTasksStats.trackHeight, perfItemWithCache, op);
                    this.hitTest.registerToggleTrack(currentX, yStart, dx, ongoingTasksStats.trackHeight, trackName); 
                }
            }

            // 3. Draw inner/nested operations/stripes..
            if (op.Operations.length > 0) {
                this.drawStripes(context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc, perfItemWithCache, trackName);
            }

            // 4. Handle errors if exist... 
            if (perfItemWithCache.HasErrors && isRootOperation) {
                context.fillStyle = this.colors.itemWithError;
                graphHelper.drawErrorMark(context, currentX, yStart, dx);
            }

            currentX += dx;
        }
    }
    
    private drawTracksNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 14.5;
        const textStart = 3 + 8 + 4;

        this.filteredTrackNames().forEach(trackName => {
            context.font = "bold 12px Lato";
            const trackType = this.getTaskType(trackName);
            const trackDescription = this.getTaskTypeDescription(trackType);

            const directionTextWidth = context.measureText(trackDescription).width;
            let restOfText = ": " + trackName;
            const restOfTextWidth = context.measureText(restOfText).width;

            const rectWidth = directionTextWidth + restOfTextWidth + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4; /* padding between arrow and text */

            context.fillStyle = this.colors.trackNameBg;
            context.fillRect(2, yScale(trackName) + ongoingTasksStats.closedTrackPadding, rectWidth, ongoingTasksStats.trackHeight);
            
            this.hitTest.registerToggleTrack(2, yScale(trackName), rectWidth, ongoingTasksStats.trackHeight, trackName);
            
            context.fillStyle = this.colors.trackDirectionText; 
            context.fillText(trackDescription, textStart + 0.5, yScale(trackName) + textShift);
            context.fillStyle = this.colors.trackNameFg;
            context.fillText(restOfText, textStart + directionTextWidth + 0.5, yScale(trackName) + textShift);

            const isOpened = _.includes(this.expandedTracks(), trackName);
            context.fillStyle = isOpened ? this.colors.openedTrackArrow : this.colors.closedTrackArrow;
            graphHelper.drawArrow(context, 5, yScale(trackName) + 6, !isOpened);
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

            context.moveTo(gapX, ongoingTasksStats.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                this.hitTest.registerGapItem(gapX - 5, ongoingTasksStats.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }
    
    private handlePreviewScript(context: previewEtlScriptItemContext) {
        this.definitionsCache.showDefinitionFor(context.etlType, context.taskId, context.transformationName);
    }

    private onToggleTrack(trackName: string) {
        if (_.includes(this.expandedTracks(), trackName)) {
            this.expandedTracks.remove(trackName);
        } else {
            this.expandedTracks.push(trackName);
        }

        this.drawMainSection();
    }

    expandAll() {
        this.expandedTracks(this.tracksInfo().map(x => x.name));
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

    private handleTrackTooltip(context: trackItemContext, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== context.item) {
            const type = context.rootStats.Type;
            const isReplication = type === "OutgoingPull"
                || type === "OutgoingPush"    
                || type === "IncomingPull"
                || type === "IncomingPush";
            const isEtl = type === "Raven" || type === "Sql";
            const isRootItem = context.rootStats.Details === context.item;
            
            let sectionName = context.item.Name;
            if (isRootItem) {
                sectionName = this.getTaskTypeDescription(type);
            }
            
            let tooltipHtml = `<strong>*** ${sectionName} ***</strong><br/>`;
            tooltipHtml += (isRootItem ? "Total duration" : "Duration") + ": " + generalUtils.formatMillis(context.item.DurationInMs) + "<br/>";
            
            if (isRootItem) {
                switch (type) {
                    case "IncomingPush":
                    case "IncomingPull": {
                        const elementWithData = context.rootStats as any as Raven.Client.Documents.Replication.IncomingReplicationPerformanceStats;
                        tooltipHtml += `Received last Etag: ${elementWithData.ReceivedLastEtag}<br/>`;
                        tooltipHtml += `Network input count: ${elementWithData.Network.InputCount}<br/>`;
                        tooltipHtml += `Documents read count: ${elementWithData.Network.DocumentReadCount}<br/>`;
                        tooltipHtml += `Attachments read count: ${elementWithData.Network.AttachmentReadCount}<br/>`;
                    }
                        break;
                    case "OutgoingPush":
                    case "OutgoingPull": {
                        const elementWithData = context.rootStats as any as Raven.Client.Documents.Replication.OutgoingReplicationPerformanceStats;
                        tooltipHtml += `Sent last Etag: ${elementWithData.SendLastEtag}<br/>`;
                        tooltipHtml += `Storage input count: ${elementWithData.Storage.InputCount}<br/>`;
                        tooltipHtml += `Documents output count: ${elementWithData.Network.DocumentOutputCount}<br/>`;
                        tooltipHtml += `Attachments read count: ${elementWithData.Network.AttachmentOutputCount}<br/>`;
                    }
                        break;
                    case "Raven":
                    case "Sql":
                        const elementWithData = context.rootStats as EtlPerformanceBaseWithCache;
                        if (elementWithData.BatchCompleteReason) {
                            tooltipHtml += `Batch complete reason: ${elementWithData.BatchCompleteReason}<br/>`;
                        }
                        
                        if (elementWithData.CurrentlyAllocated && elementWithData.CurrentlyAllocated.SizeInBytes) {
                            tooltipHtml += `Currently allocated: ${generalUtils.formatBytesToSize(elementWithData.CurrentlyAllocated.SizeInBytes)} <br/>`;
                        }
                        break;
                }
                
                if (isReplication) {
                    const baseElement = context.rootStats as Raven.Client.Documents.Replication.ReplicationPerformanceBase;
                    if (baseElement.Errors) {
                        tooltipHtml += `<span style=color:Crimson;"><strong>Errors:</strong></span><br/>`;
                        baseElement.Errors.forEach(err => tooltipHtml += `Errors: ${err.Error}<br/>`);
                    }
                }
            } else { // child item
                if (isEtl) {
                    const baseElement = context.rootStats as EtlPerformanceBaseWithCache;
                    switch (context.item.Name) {
                        case "Extract":
                            _.forIn(baseElement.NumberOfExtractedItems, (value: number, key: Raven.Server.Documents.ETL.EtlItemType) => {
                                if (value) {
                                    tooltipHtml += `Extracted ${ongoingTasksStats.etlItemTypeToUi(key)}: ${value.toLocaleString()}<br/>`;
                                }
                            });

                            _.forIn(baseElement.LastFilteredOutEtags, (value: number, key: Raven.Server.Documents.ETL.EtlItemType) => {
                                if (value) {
                                    tooltipHtml += `Last filtered out Etag for ${key}: ${value}<br/>`;
                                }
                            });
                            break;
                        case "Transform":
                            _.forIn(baseElement.NumberOfTransformedItems, (value: number, key: Raven.Server.Documents.ETL.EtlItemType) => {
                                if (value) {
                                    tooltipHtml += `Transformed ${ongoingTasksStats.etlItemTypeToUi(key)}: ${value.toLocaleString()}<br/>`;
                                    
                                    if (baseElement.DurationInMs) {
                                        const durationInSec = context.item.DurationInMs / 1000;
                                        tooltipHtml += `${ongoingTasksStats.etlItemTypeToUi(key)} Processing Speed: ${Math.floor(value / durationInSec).toLocaleString()} docs/sec<br/>`;
                                    }
                                }
                            });

                            _.forIn(baseElement.NumberOfTransformedTombstones, (value: number, key: Raven.Server.Documents.ETL.EtlItemType) => {
                                if (value) {
                                    tooltipHtml += `Transformed ${ongoingTasksStats.etlItemTypeToUi(key)} tombstones: ${value.toLocaleString()}<br/>`;
                                }
                            });
                            
                            if (baseElement.TransformationErrorCount) {
                                tooltipHtml += `Transformation error count: ${baseElement.TransformationErrorCount.toLocaleString()}<br/>`;
                            }

                            _.forIn(baseElement.LastTransformedEtags, (value: number, key: Raven.Server.Documents.ETL.EtlItemType) => {
                                if (value) {
                                    tooltipHtml += `Last transformed Etag for ${key}: ${value}<br/>`;
                                }
                            });
                            break;
                        case "Load":
                            if (baseElement.SuccessfullyLoaded != null) {
                                tooltipHtml += `Successfully loaded: ${baseElement.SuccessfullyLoaded ? "Yes": "No"}<br/>`;
                            }

                            if (baseElement.LastLoadedEtag) {
                                tooltipHtml += `Last loaded Etag: ${baseElement.LastLoadedEtag}<br/>`;
                            }
                            break;
                    }
                }
            }
            
            this.handleTooltip(context.item, x, y, tooltipHtml); 
        }
    }
    
    static etlItemTypeToUi(value: Raven.Server.Documents.ETL.EtlItemType) {
        switch (value) {
            case "Document":
                return "Documents";
            case "Counter":
                return "Counters";
            default:
                return "None";
        }
    }

    private handleTooltip(element: taskOperation | timeGapInfo | performanceBaseWithCache,
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
            self.dataImported(this.result as string);
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
            let importedData: exportFileFormat = JSON.parse(result);
            
            if (_.isArray(importedData)) {
                // maybe we imported old format let's try to convert
                importedData = {
                    Replication: importedData as any, // we force casting here
                    Etl: []
                }
            }

            // Data validation (currently only checking if this is an array, may do deeper validation later..
            if (!_.isObject(importedData)) { 
                messagePublisher.reportError("Invalid replication stats file format", undefined, undefined);
            } else {
                this.replicationData = importedData.Replication;
                this.etlData = importedData.Etl;

                this.fillCache();
                this.prepareBrush(); 
                this.resetGraphData();
                const [workData, maxConcurrentActions] = this.prepareTimeData();
                this.draw(workData, maxConcurrentActions, true);

                this.isImport(true);
            }
        } catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
        }
    }

    private fillCache() {
        this.replicationData.forEach(replicationStat => {
            replicationStat.Performance.forEach(perfStat => {
                liveReplicationStatsWebSocketClient.fillCache(perfStat, replicationStat.Type, replicationStat.Description);
            });
        });
        
        this.etlData.forEach(etlTaskData => {
            etlTaskData.Stats.forEach(etlStats => {
                etlStats.Performance.forEach(perfStat => {
                    liveEtlStatsWebSocketClient.fillCache(perfStat, etlTaskData.EtlType);
                });
            })
        });
    }

    clearGraphWithConfirm() {
        this.confirmationMessage("Clear graph data", "Do you want to discard all collected ongoing tasks statistics?")
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
        const replicationMax = d3.max(this.replicationData,
            d => d3.max(d.Performance,
                (p: ReplicationPerformanceBaseWithCache) => p.StartedAsDate));
        
        const etlMax = d3.max(this.etlData, 
                taskData => d3.max(taskData.Stats,
                        stats => d3.max(stats.Performance, 
                            (p: EtlPerformanceBaseWithCache) => p.StartedAsDate)));
        
        this.dateCutoff = d3.max([replicationMax, etlMax]);
    }

    closeImport() {
        this.dateCutoff = null;
        this.isImport(false);
        this.clearGraph();
    }

    private resetGraphData() {
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());

        this.expandedTracks([]);
        this.searchText("");
        this.bufferUsage("0.0");
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
            exportFileName = `OngoingTasksStats of ${this.activeDatabase().name} ${moment().format("YYYY-MM-DD HH-mm")}`;
        }

        const keysToIgnore: Array<keyof performanceBaseWithCache> = ["StartedAsDate", "CompletedAsDate"];
        const filePayload: exportFileFormat = {
            Replication: this.replicationData,
            Etl: this.etlData
        };
        fileDownloader.downloadAsJson(filePayload, exportFileName + ".json", exportFileName, (key, value) => {
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

export = ongoingTasksStats;

