import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");
import rbush = require("rbush");
import gapFinder = require("common/helpers/graph/gapFinder");
import generalUtils = require("common/generalUtils");
import rangeAggregator = require("common/helpers/graph/rangeAggregator");
import liveEtlStatsWebSocketClient = require("common/liveEtlStatsWebSocketClient");
import messagePublisher = require("common/messagePublisher");
import inProgressAnimator = require("common/helpers/graph/inProgressAnimator");
import colorsManager = require("common/colorsManager");
import etlScriptDefinitionCache = require("models/database/stats/etlScriptDefinitionCache");
import fileImporter = require("common/fileImporter");
import moment = require("moment");
import database = require("models/resources/database");
import TaskUtils = require("components/utils/TaskUtils");
import DatabaseUtils = require("components/utils/DatabaseUtils");
import showDataDialog = require("viewmodels/common/showDataDialog");
import app = require("durandal/app");
import typeUtils = require("common/typeUtils");
import shardViewModelBase = require("viewmodels/shardViewModelBase");
import EtlType = Raven.Client.Documents.Operations.ETL.EtlType;

type treeActionType = "toggleTrack" | "trackItem" | "gapItem" | "previewEtlScript" | "previewSinkScript" |
                      "subscriptionErrorItem" | "subscriptionPendingItem" | "subscriptionConnectionItem" | "previewSubscriptionQuery";

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: treeActionType;
    arg: any;
}

type taskOperation = Raven.Client.Documents.Replication.ReplicationPerformanceOperation |
                     Raven.Server.Documents.ETL.Stats.EtlPerformanceOperation |
                     Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkPerformanceOperation |
                     Raven.Server.Documents.Subscriptions.Stats.SubscriptionConnectionPerformanceOperation |
                     Raven.Server.Documents.Subscriptions.Stats.SubscriptionBatchPerformanceOperation;

type performanceBaseWithCache = ReplicationPerformanceWithCache |
                                EtlPerformanceBaseWithCache |
                                QueueSinkPerformanceBaseWithCache |
                                SubscriptionConnectionPerformanceStatsWithCache |
                                SubscriptionBatchPerformanceStatsWithCache;
type trackInfo = {
    name: string;
    type: ongoingTaskStatType;
    openedHeight: number;
    closedHeight: number;
}

type exportFileFormat = {
    Replication: Raven.Server.Documents.Replication.Stats.LiveReplicationPerformanceCollector.ReplicationPerformanceStatsBase<Raven.Client.Documents.Replication.ReplicationPerformanceBase>[];
    Etl: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats[];
    Subscription: Raven.Server.Documents.Subscriptions.SubscriptionTaskPerformanceStats[];
    QueueSink: Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkTaskPerformanceStats[];
}

type trackItemContext = {
    rootStats: performanceBaseWithCache;
    item: taskOperation;
}

type previewEtlScriptItemContext = {
    transformationName: string;
    taskId: number;
    etlType: EtlType;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleTrack: (trackName: string) => void;
    private onPreviewEtlScript: (context: previewEtlScriptItemContext) => void;
    private handleTrackTooltip: (context: trackItemContext, x: number, y: number) => void;
    private handleSubscriptionErrorTooltip: (context: subscriptionErrorItemInfo, x: number, y: number) => void;
    private handleSubscriptionPendingTooltip: (context: subscriptionPendingItemInfo, x: number, y: number) => void;
    private handleSubscriptionConnectionTooltip: (context: subscriptionConnectionItemInfo, x: number, y: number) => void;
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleTrack: (trackName: string) => void,
        onPreviewEtlScript: (context: previewEtlScriptItemContext) => void,
        handleTrackTooltip: (context: trackItemContext, x: number, y: number) => void,
        handleSubscriptionErrorTooltip: (context: subscriptionErrorItemInfo, x: number, y: number) => void,
        handleSubscriptionPendingTooltip: (context: subscriptionPendingItemInfo, x: number, y: number) => void,
        handleSubscriptionConnectionTooltip: (context: subscriptionConnectionItemInfo, x: number, y: number) => void,
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {
        this.container = container;
        this.onToggleTrack = onToggleTrack;
        this.onPreviewEtlScript = onPreviewEtlScript;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleSubscriptionErrorTooltip = handleSubscriptionErrorTooltip;
        this.handleSubscriptionPendingTooltip = handleSubscriptionPendingTooltip;
        this.handleSubscriptionConnectionTooltip = handleSubscriptionConnectionTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, rootStats: performanceBaseWithCache, op: taskOperation) {
        const trackInfoItem = { rootStats: rootStats, item: op } as trackItemContext;
        this.insertItem(x, y, width, height, "trackItem", trackInfoItem);
    }

    registerPreviewEtlScript(x: number, y: number, width: number, height: number, taskInfo: previewEtlScriptItemContext) {
        this.insertItem(x, y, width, height, "previewEtlScript", taskInfo);
    }

    registerToggleTrack(x: number, y: number, width: number, height: number, trackName: string) {
        this.insertItem(x, y, width, height, "toggleTrack", trackName);
    }

    registerGapItem(x: number, y: number, width: number, height: number, gapInfo: timeGapInfo) {
        this.insertItem(x, y, width, height, "gapItem", gapInfo);
    }

    private insertItem(x: number, y: number, width: number, height: number, action: treeActionType, args: any) {
        const item: rTreeLeaf = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: action,
            arg: args
        };

        this.rTree.insert(item);
    }

    onClick() {
        const clickLocation = d3.mouse(this.container.node());

        if ((d3.event as any).defaultPrevented) {
            return;
        }

        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const previewEtlScript = items.find(x => x.actionType === "previewEtlScript");
        if (previewEtlScript) {
            this.onPreviewEtlScript(previewEtlScript.arg as previewEtlScriptItemContext);
            return;
        }

        const toggleTrack = items.find(x => x.actionType === "toggleTrack");
        if (toggleTrack) {
            this.onToggleTrack(toggleTrack.arg as string);
        }
    }

    onMouseDown() {
        if (!this.overTooltip()) {
            this.removeTooltip();
        }
        this.cursor(graphHelper.prefixStyle("grabbing"));
    }

    onMouseUp() {
        this.cursor(graphHelper.prefixStyle("grab"));
    }
    private overTooltip(): boolean {
        const tooltip = document.querySelector(".tooltip");
        if (!tooltip) {
            return false;
        }

        const [mouseX, mouseY] = d3.mouse(document.querySelector("body"));
        const tooltipPosition = tooltip.getBoundingClientRect();
        if (mouseX < tooltipPosition.x - 2 || mouseX > tooltipPosition.x + tooltipPosition.width) {
            return false;
        }

        if (mouseY < tooltipPosition.y - 2 || mouseY > tooltipPosition.y + tooltipPosition.height) {
            return false;
        }

        return true;
    }

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        if (this.overTooltip()) {
            // over tooltip - do nothing
            return;
        }

        const overToggleTrack = items.find(x => x.actionType === "toggleTrack");

        const currentPreviewEtlItem = items.find(x => x.actionType === "previewEtlScript");
        if (currentPreviewEtlItem) {
            this.cursor("pointer");
            return;
        }

        const currentPreviewSinkItem = items.find(x => x.actionType === "previewSinkScript");
        if (currentPreviewSinkItem) {
            this.cursor("pointer");
            return;
        }

        const currentPreviewSubscriptionItem = items.find(x => x.actionType === "previewSubscriptionQuery");
        if (currentPreviewSubscriptionItem) {
            this.cursor("pointer");
            return;
        }

        const currentTrackEventItem = items.find(x => x.actionType === "subscriptionErrorItem");
        if (currentTrackEventItem) {
            this.handleSubscriptionErrorTooltip(currentTrackEventItem.arg as subscriptionErrorItemInfo , clickLocation[0], currentTrackEventItem.maxY + aiTasksStats.brushSectionHeight);
            this.cursor("auto");
            return;
        }

        const currentTrackPendingItem = items.find(x => x.actionType === "subscriptionPendingItem");
        if (currentTrackPendingItem) {
            this.handleSubscriptionPendingTooltip(currentTrackPendingItem.arg as subscriptionPendingItemInfo, clickLocation[0], currentTrackPendingItem.maxY + aiTasksStats.brushSectionHeight);
            this.cursor("auto");
            return;
        }

        const currentTrackConnectionItem = items.find(x => x.actionType === "subscriptionConnectionItem");
        if (currentTrackConnectionItem) {
            this.handleSubscriptionConnectionTooltip(currentTrackConnectionItem.arg as subscriptionConnectionItemInfo, clickLocation[0], currentTrackConnectionItem.maxY + aiTasksStats.brushSectionHeight);
            this.cursor("auto");
            return;
        }

        const currentTrackItem = items.find(x => x.actionType === "trackItem");
        if (currentTrackItem) {
            this.handleTrackTooltip(currentTrackItem.arg as trackItemContext, clickLocation[0], clickLocation[1]);
            this.cursor("auto");
            return;
        }

        const currentGapItem = items.find(x => x.actionType === "gapItem");
        if (currentGapItem) {
            this.handleGapTooltip(currentGapItem.arg as timeGapInfo, clickLocation[0], clickLocation[1]);
            this.cursor("auto");
            return;
        }

        this.removeTooltip();
        this.cursor(overToggleTrack ? "pointer" : graphHelper.prefixStyle("grab"));
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - aiTasksStats.brushSectionHeight,
            maxY: y - aiTasksStats.brushSectionHeight
        });
    }
}


class aiTasksStats extends shardViewModelBase {
    view = require("views/database/aiHub/aiTasksStats.html");


    private static readonly showDetailsButton = `<div class="margin-left-sm">
    <a href="#" class="btn btn-default btn-sm js-task-details-btn" title="Show details">
        <i class="icon-preview"></i>
    </a>
</div>`;

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
    private static readonly textLeftPadding = 14;
    private static readonly previewIconWidth = 16;

    private static readonly maxEtlRecursion = 2;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly bufferSize = 10000;

    private static readonly singleOpenedEtlItemHeight = aiTasksStats.maxEtlRecursion * aiTasksStats.trackHeight
        + (aiTasksStats.maxEtlRecursion - 1) * aiTasksStats.stackPadding;

    private static readonly olapLoadLocalPrefix = "Load/Local/";
    private static readonly olapLoadLocalChild = "Load/Local/Child";

    private static readonly olapUploadPrefix = "Load/Upload/";
    private static readonly olapUploadChild = "Load/Upload/Child";

    /* observables */

    hasAnyData = ko.observable<boolean>(false);
    loading: KnockoutComputed<boolean>;
    private searchText = ko.observable<string>("");

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
    private etlData: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats[] = [];

    private etlDefinitionsCache: etlScriptDefinitionCache;

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
    private firstDataChunkDrawn = false;

    private currentDetails: string = null;

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
    private tooltip: d3.Selection<taskOperation | timeGapInfo | performanceBaseWithCache | subscriptionErrorItemInfo | subscriptionPendingItemInfo>;

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
        stripeTextColor: undefined as string,

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
            "Storage/TimeSeriesRead": undefined as string,
            "ETL": undefined as string,
            "Extract": undefined as string,
            "Transform": undefined as string,
            "Load" : undefined as string,
            "Load/Local" : undefined as string,
            "Load/Local/Child" : undefined as string,
            "Load/Upload" : undefined as string,
            "Load/Upload/Child" : undefined as string,
            "ConnectionPending": undefined as string,
            "ConnectionActive": undefined as string,
            "Batch": undefined as string,
            "BatchSendDocuments": undefined as string,
            "BatchWaitForAcknowledge": undefined as string,
            "ConnectionAborted": undefined as string,
            "ConnectionRejected": undefined as string,
            "ConnectionErrorBackground": undefined as string,
            "AggregatedBatchesInfo": undefined as string,
            "Consume": undefined as string,
            "QueueReading": undefined as string,
            "ScriptProcessing": undefined as string,
            "UnknownOperation": undefined as string,
        }
    };

    constructor(db: database, location: databaseLocationSpecifier) {
        super(db, location);

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
            const etlClient = this.liveViewEtlClient();
            return etlClient ? etlClient.loading() : true;
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

        if (this.liveViewEtlClient()) {
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

        const activeDatabase = this.db;
        this.etlDefinitionsCache = new etlScriptDefinitionCache(activeDatabase);

        this.hitTest.init(this.svg,
            (replicationName) => this.onToggleTrack(replicationName),
            (context) => this.handlePreviewEtlScript(context),
            (context, x, y) => this.handleTrackTooltip(context, x, y),
            (context, x, y) => this.handleSubscriptionErrorTooltip(context, x, y),
            (context, x, y) => this.handleSubscriptionPendingTooltip(context, x, y),
            (context, x, y) => this.handleSubscriptionConnectionTooltip(context, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();

        const $body = $("body");
        this.registerDisposableDelegateHandler($body, "click", ".js-task-details-btn", (event: JQuery.TriggeredEvent) => {
            event.preventDefault();
            app.showBootstrapDialog(new showDataDialog("Error details", this.currentDetails, "plain"));

            this.hideTooltip();
        });
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
            .attr("height", this.totalHeight - aiTasksStats.brushSectionHeight - aiTasksStats.axisHeight)
            .style("top", (aiTasksStats.brushSectionHeight + aiTasksStats.axisHeight) + "px");

        const inProgressCanvasNode = this.inProgressCanvas.node() as HTMLCanvasElement;
        const inProgressContext = inProgressCanvasNode.getContext("2d");
        inProgressContext.translate(0, -aiTasksStats.axisHeight);

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
            .attr("height", this.totalHeight - aiTasksStats.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + aiTasksStats.brushSectionHeight + ")")
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
                if (this.liveViewEtlClient()) {
                    this.liveViewEtlClient().pauseUpdates();
                }
                this.updatesPaused = true;
            });
        selection
            .on("mouseup.hit", () => {
                this.hitTest.onMouseUp();
                selection.on("mousemove.tip", onMove);
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
        if (this.firstDataChunkDrawn) {
            const timeToRemap: [number, number] = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
            // noinspection JSSuspiciousNameCombination
            timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        }

        this.checkBufferUsage();

        const [workData, maxConcurrentActions] = this.prepareTimeData();

        if (this.firstDataChunkDrawn) {
            const newBrush = timeRange.map(x => this.xBrushTimeScale(x)) as [number, number];
            this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
        }

        if (this.autoScroll()) {
            this.scrollToRight();
        }

        this.draw(workData, maxConcurrentActions, !this.firstDataChunkDrawn);

        if (!this.firstDataChunkDrawn) {
            this.firstDataChunkDrawn = true;
        }
    }

    private enableLiveView() {
        this.firstDataChunkDrawn = false;

        // since we are fetching data from 3 different sources
        // let's throttle updates to avoid jumpy UI
        const onDataUpdatedThrottle = _.debounce(() => {
            if (!this.updatesPaused) {
                this.onDataUpdated();
            }
        }, 1000, { maxWait: 3000 });

        this.liveViewEtlClient(new liveEtlStatsWebSocketClient(this.db, this.location, d => {
            const aiEtlData = d.filter(x => x.EtlType === "EmbeddingsGeneration")
            this.etlData = aiEtlData;
            onDataUpdatedThrottle();
        }, this.dateCutoff));
    }

    private checkBufferUsage() {
        const etlDataCount = typeUtils.sumBy(this.etlData, t => typeUtils.sumBy(t.Stats, s => s.Performance.length));

        const usage = Math.min(100, etlDataCount * 100.0 / aiTasksStats.bufferSize);
        this.bufferUsage(usage.toFixed(1));

        if (etlDataCount > aiTasksStats.bufferSize) {
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
        if (this.liveViewEtlClient()) {
            this.liveViewEtlClient().dispose();
            this.liveViewEtlClient(null);
        }
    }

    private draw(workData: workData[], maxConcurrentActions: number, resetFilter: boolean) {
        this.hasAnyData(this.etlData.length > 0);

        this.prepareBrushSection(workData, maxConcurrentActions);
        this.prepareMainSection(resetFilter);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.clearRect(0, 0, this.totalWidth + 2 /* aliasing */, aiTasksStats.brushSectionHeight);
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

        this.gapFinder = new gapFinder(timeRanges, aiTasksStats.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        return [workData, maxConcurrentActions];
    }

    private prepareBrushSection(workData: workData[], maxConcurrentActions: number) {
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth + 1;
        this.brushSection.height = aiTasksStats.brushSectionHeight;

        this.yBrushValueScale = d3.scale.linear()
            .domain([0, maxConcurrentActions])
            .range([0, aiTasksStats.brushSectionTrackWorkHeight]);

        const context = this.brushSection.getContext("2d");

        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, aiTasksStats.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = this.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth, aiTasksStats.brushSectionHeight - 1);

        context.fillStyle = this.colors.brushChartColor;
        context.strokeStyle = this.colors.brushChartStrokeColor;
        context.lineWidth = aiTasksStats.brushSectionLineWidth;

        // Draw area chart showing replication work
        let x1: number, x2: number, y0 = 0, y1: number;
        for (let i = 0; i < workData.length - 1; i++) {

            context.beginPath();
            x1 = this.xBrushTimeScale(new Date(workData[i].pointInTime));
            y1 = Math.round(this.yBrushValueScale(workData[i].numberOfItems)) + 0.5;
            x2 = this.xBrushTimeScale(new Date(workData[i + 1].pointInTime));
            context.moveTo(x1, aiTasksStats.brushSectionHeight - y0);
            context.lineTo(x1, aiTasksStats.brushSectionHeight - y1);

            // Don't want to draw line -or- rect at level 0
            if (y1 !== 0) {
                context.lineTo(x2, aiTasksStats.brushSectionHeight - y1);
                context.fillRect(x1, aiTasksStats.brushSectionHeight - y1, x2 - x1, y1);
            }

            context.stroke();
            y0 = y1;
        }

        // Draw last line:
        context.beginPath();
        context.moveTo(x2, aiTasksStats.brushSectionHeight - y1);
        context.lineTo(x2, aiTasksStats.brushSectionHeight);
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
            context.lineTo(gapX, aiTasksStats.brushSectionHeight - 2);
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
                .attr("height", aiTasksStats.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilter: boolean): void {
        this.findAndSetTaskNames();

        if (resetFilter) {
            this.searchText("");
        }
        this.filterTracks();
    }

    private findAndSetTaskNames(): void {
        this.etlData = _.orderBy(this.etlData, [(x: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats) => x.EtlType, (x: Raven.Server.Documents.ETL.Stats.EtlTaskPerformanceStats) => x.TaskName], ["asc", "asc"]);

        this.etlData.forEach(etl => {
            etl.Stats = _.orderBy(etl.Stats, [(x: any) => x.TransformationName], ["asc"]);
        });

        const trackInfos: trackInfo[] = [];


        this.etlData.forEach(etlTask => {
            const scriptsCount = etlTask.Stats.length;

            const closedHeight = aiTasksStats.openedTrackPadding
                + (scriptsCount + 1) * aiTasksStats.trackHeight
                + scriptsCount * aiTasksStats.betweenScriptsPadding
                + aiTasksStats.openedTrackPadding;


            const openedHeight = 2 * aiTasksStats.openedTrackPadding
                + aiTasksStats.trackHeight
                + (scriptsCount - 1) * aiTasksStats.betweenScriptsPadding
                + scriptsCount * aiTasksStats.singleOpenedEtlItemHeight
                + aiTasksStats.openedTrackPadding;

                trackInfos.push({
                    name: etlTask.TaskName,
                    type: etlTask.EtlType,
                    openedHeight: openedHeight,
                    closedHeight: closedHeight,
                });
        });

        this.tracksInfo(trackInfos);
    }

    private fixCurrentOffset(): void {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale(): void {
        let currentOffset = aiTasksStats.axisHeight - this.currentYOffset;
        const domain: string[] = [];
        const range: number[] = [];

        const trackNames = this.filteredTrackNames();

        for (let i = 0; i < trackNames.length; i++) {
            const trackName = trackNames[i];

            domain.push(trackName);
            range.push(currentOffset);

            const isOpened = _.includes(this.expandedTracks(), trackName);

            const trackInfo = this.tracksInfo().find(x => x.name === trackName);

            currentOffset += (isOpened ? trackInfo.openedHeight : trackInfo.closedHeight) + aiTasksStats.trackMargin;
        }

        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset(): void {
        const heightSum = typeUtils.sumBy(this.filteredTrackNames(), track => {
            const isOpened = _.includes(this.expandedTracks(), track);
            const trackInfo = this.tracksInfo().find(x => x.name === track);
            return isOpened ? trackInfo.openedHeight : trackInfo.closedHeight;
        });

        const offset = aiTasksStats.axisHeight
            + this.filteredTrackNames().length * aiTasksStats.trackMargin
            + heightSum;

        const availableHeightForTracks = this.totalHeight - aiTasksStats.brushSectionHeight;

        const extraBottomMargin = 10;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private getTicks(scale: d3.time.Scale<number, number>): Date[] {
        return d3.range(aiTasksStats.initialOffset, this.totalWidth - aiTasksStats.step, aiTasksStats.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number): void {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = this.colors.axis;

            ticks.forEach((_, i) => {
                context.moveTo(aiTasksStats.initialOffset + (i * aiTasksStats.step) + 0.5, yStart);
                context.lineTo(aiTasksStats.initialOffset + (i * aiTasksStats.step) + 0.5, yEnd);
            });

            context.stroke();
        }
        finally {
            context.restore();
        }
    }

    private drawXaxisTimeLabels(context: CanvasRenderingContext2D, ticks: Date[], timePaddingLeft: number, timePaddingTop: number): void {
        try {
            context.save();
            context.beginPath();

            context.textAlign = "left";
            context.textBaseline = "top";
            context.font = "10px 'Figtree', 'Helvetica Neue', Helvetica, Arial, sans-serif";
            context.fillStyle = this.colors.axis;

            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), aiTasksStats.initialOffset + (i * aiTasksStats.step) + timePaddingLeft, timePaddingTop);
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
        const result: Array<[Date, Date]> = [];

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
            context.translate(0, aiTasksStats.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth + 2 /* aliasing */, this.totalHeight - aiTasksStats.brushSectionHeight);

            this.drawTracksBackground(context);

            if (xScale.domain().length) {

                const ticks = this.getTicks(xScale);

                context.save();
                context.beginPath();
                context.rect(0, aiTasksStats.axisHeight - 3, this.totalWidth, this.totalHeight - aiTasksStats.brushSectionHeight);
                context.clip();
                const timeYStart = this.yScale.range()[0] || aiTasksStats.axisHeight;
                this.drawXaxisTimeLines(context, ticks, timeYStart - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            context.save();
            try {
                context.beginPath();
                context.rect(0, aiTasksStats.axisHeight, this.totalWidth, this.totalHeight - aiTasksStats.brushSectionHeight);
                context.clip();

                this.drawTracks(context, xScale, visibleTimeFrame);
                this.drawTracksNames(context);
                this.drawGaps(context, xScale);

                graphHelper.drawScroll(context,
                    { left: this.totalWidth, top: aiTasksStats.axisHeight },
                    this.currentYOffset,
                    this.totalHeight - aiTasksStats.brushSectionHeight - aiTasksStats.axisHeight,
                    this.maxYOffset ? this.maxYOffset + this.totalHeight - aiTasksStats.brushSectionHeight - aiTasksStats.axisHeight : 0,
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
        context.rect(0, aiTasksStats.axisHeight, this.totalWidth, this.totalHeight - aiTasksStats.brushSectionHeight);
        context.clip();

        const drawBackground = (trackName: string) => {
            const yStart = this.yScale(trackName);
            const isOpened = _.includes(this.expandedTracks(), trackName);
            const trackInfo = this.tracksInfo().find(x => x.name === trackName);

            if (trackInfo) {
                context.beginPath();
                context.fillStyle = this.colors.trackBackground;
                context.fillRect(0, yStart, this.totalWidth, isOpened ? trackInfo.openedHeight : trackInfo.closedHeight);
            }
        };

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
            yStart += isOpened ? aiTasksStats.openedTrackPadding : aiTasksStats.closedTrackPadding;

            const perfLength = performance.length;
            let perfCompleted: string = null;

            for (let perfIdx = 0; perfIdx < perfLength; perfIdx++) {
                const perf = performance[perfIdx];   // each performance[i] has:  completed, details, DurationInMilliseconds, id, started

                const perfWithCache = perf as performanceBaseWithCache; // cache has also: startedAsDate, CompletedAsDate, Type
                const startDate = perfWithCache.StartedAsDate;

                const x1 = xScale(startDate);
                const startDateAsInt = startDate.getTime();

                const endDateAsInt = startDateAsInt + perf.DurationInMs;
                if (endDateAsInt < visibleStartDateAsInt || visibleEndDateAsInt < startDateAsInt) {
                    continue;
                }

                const yOffset = isOpened ? aiTasksStats.trackHeight + aiTasksStats.stackPadding : 0;
                const stripesYStart = yStart + (isOpened ? yOffset : 0);

                context.save();

                // Draw perf items
                if (perfWithCache.Details) {
                this.drawStripes(context, [perfWithCache.Details], x1, stripesYStart, yOffset, extentFunc, perfWithCache, trackName);
                }

                // Draw a separating line between adjacent perf items if needed
                if (perfIdx >= 1 && perfCompleted === perf.Started) {
                    context.fillStyle = this.colors.separatorLine;
                    context.fillRect(x1, yStart + (isOpened ? yOffset : 0), 1, aiTasksStats.trackHeight);
                }

                context.restore();

                // Save to compare with the start time of the next item...
                perfCompleted = perf.Completed;

                if (!perf.Completed && perf.Details) {
                    this.findInProgressAction([perf.Details], extentFunc, x1, stripesYStart, yOffset);
                }
            }
        };

        this.etlData.forEach(etlItem => {
            const trackName = etlItem.TaskName;
            if (_.includes(this.filteredTrackNames(), trackName)) {
                const yStartBase = this.yScale(trackName);
                const isOpened = _.includes(this.expandedTracks(), trackName);
                const extraPadding = isOpened ? aiTasksStats.trackHeight + aiTasksStats.stackPadding + aiTasksStats.openedTrackPadding
                    : aiTasksStats.closedTrackPadding;

                etlItem.Stats.forEach((etlStat, idx) => {
                    context.font = "10px 'Figtree', 'Helvetica Neue', Helvetica, Arial, sans-serif";
                    const openedTrackItemOffset = aiTasksStats.betweenScriptsPadding + aiTasksStats.singleOpenedEtlItemHeight;
                    const closedTrackItemOffset = aiTasksStats.betweenScriptsPadding + aiTasksStats.trackHeight;
                    const offset = isOpened ? idx * openedTrackItemOffset : (idx + 1) * closedTrackItemOffset;

                    drawTrack(trackName, yStartBase + offset, isOpened, etlStat.Performance as performanceBaseWithCache[]);

                    this.drawEtlScriptName(context, yStartBase + offset + extraPadding, {
                        transformationName: etlStat.TransformationName,
                        etlType: etlItem.EtlType,
                        taskId: etlItem.TaskId
                    });
                });
            }
        });
    }

    private drawEtlScriptName(context: CanvasRenderingContext2D, yStart: number, taskInfo: previewEtlScriptItemContext) {
        const areaWidth = this.drawText(context, yStart, taskInfo.transformationName);
        this.hitTest.registerPreviewEtlScript(2, yStart, areaWidth, aiTasksStats.trackHeight, taskInfo);
    }

    private drawText(context: CanvasRenderingContext2D, yStart: number, text: string) {
        const textShift = 12.5;
        context.font = "bold 12px 'Figtree', 'Helvetica Neue', Helvetica, Arial, sans-serif";
        const textWidth = context.measureText(text).width + 8;

        const areaWidth = textWidth + aiTasksStats.textLeftPadding * 2 + aiTasksStats.previewIconWidth;

        context.fillStyle = this.colors.trackNameBg;
        context.fillRect(2, yStart, areaWidth, aiTasksStats.trackHeight + 2);

        context.fillStyle = this.colors.trackNameFg;
        context.fillText(text, aiTasksStats.textLeftPadding + 4, yStart + textShift);

        context.font = "16px icomoon";
        context.fillText('\uf133', aiTasksStats.textLeftPadding + textWidth + aiTasksStats.previewIconWidth / 2, yStart + 16);

        return areaWidth;
    }

    private findInProgressAction(perfDetails: taskOperation[], extentFunc: (duration: number) => number,
                                 xStart: number, yStart: number, yOffset: number): void {

        const extractor = (perfs: taskOperation[], xStart: number, yStart: number, yOffset: number) => {
            let currentX = xStart;

            perfs.forEach(op => {
                const dx = extentFunc(op.DurationInMs);

                this.inProgressAnimator.register([currentX, yStart, dx, aiTasksStats.trackHeight]);

                if (op.Operations && op.Operations.length > 0) {
                    extractor(op.Operations, currentX, yStart + yOffset, yOffset);
                }
                currentX += dx;
            });
        };

        extractor(perfDetails, xStart, yStart, yOffset);
    }

    private getColorForOperation(operationName: string): string {
        const { tracks } = this.colors;

        if (operationName.startsWith(aiTasksStats.olapLoadLocalPrefix)) {
            operationName = aiTasksStats.olapLoadLocalChild;
        }
        if (operationName.startsWith(aiTasksStats.olapUploadPrefix)) {
            operationName = aiTasksStats.olapUploadChild;
        }

        if (operationName in tracks) {
            return (tracks as dictionary<string>)[operationName];
        }

        console.warn(`Operation "${operationName}" is not supported. Using unknown-operation color in ongoing tasks graph.`);
        return tracks.UnknownOperation;
    }

    private getTaskType(taskName: string): ongoingTaskStatType {
        return this.tracksInfo().find(x => x.name === taskName).type;
    }

    private getTaskTypeDescription(type: ongoingTaskStatType): string {
        switch (type) {
            case "EmbeddingsGeneration":
                return "Embeddings Generation"
            default:
                throw new Error("Unknown stats type: " + type);
        }
        return "";
    }

    private drawStripes(context: CanvasRenderingContext2D, operations: Array<taskOperation>,
        xStart: number, yStart: number, yOffset: number, extentFunc: (duration: number) => number,
        perfItemWithCache: performanceBaseWithCache, trackName: string) {

        let currentX = xStart;

        for (let i = 0; i < operations.length; i++) {
            const op = operations[i];
            const dx = extentFunc(op.DurationInMs);
            const isRootOperation = perfItemWithCache.Details === op;

            // 0. Draw item:
            context.fillStyle = this.getColorForOperation(op.Name);
            context.fillRect(currentX, yStart, dx, aiTasksStats.trackHeight);

            // Register items:
            // 1. Track is open
            if (yOffset !== 0) {
                if (dx >= 0.8) { // Don't show tooltip & text for small items
                    this.hitTest.registerTrackItem(currentX, yStart, dx, aiTasksStats.trackHeight, perfItemWithCache, op);

                    if (dx > 30) {
                        this.drawTextOnStripe(context, op.Name, dx, currentX, yStart);
                    }
                }
            }
            // 2. Track is closed
            else if (isRootOperation) { // register only on root item
                if (dx >= 0.8) {
                    this.hitTest.registerTrackItem(currentX, yStart, dx, aiTasksStats.trackHeight, perfItemWithCache, op);
                    this.hitTest.registerToggleTrack(currentX, yStart, dx, aiTasksStats.trackHeight, trackName);
                }
            }

            // 3. Draw inner/nested operations/stripes...
            if (op.Operations && op.Operations.length > 0) {
                this.drawStripes(context, op.Operations, currentX, yStart + yOffset, yOffset, extentFunc, perfItemWithCache, trackName);
            }

            // 4. Handle errors if exist...
            if (perfItemWithCache.HasErrors && isRootOperation) {
                context.fillStyle = this.colors.itemWithError;
                graphHelper.drawTriangle(context, currentX, yStart, dx);
            }

            currentX += dx;
        }
    }

    private drawTracksNames(context: CanvasRenderingContext2D) {
        const yScale = this.yScale;
        const textShift = 14.5;
        const textStart = 3 + 8 + 4;

        this.filteredTrackNames().forEach(trackName => {
            context.font = "bold 12px 'Figtree', 'Helvetica Neue', Helvetica, Arial, sans-serif";
            const trackType = this.getTaskType(trackName);
            const trackDescription = this.getTaskTypeDescription(trackType);

            const directionTextWidth = context.measureText(trackDescription).width;
            const restOfText = ": " + trackName;
            const restOfTextWidth = context.measureText(restOfText).width;

            const rectWidth = directionTextWidth + restOfTextWidth + 2 * 3 /* left right padding */ + 8 /* arrow space */ + 4; /* padding between arrow and text */

            context.fillStyle = this.colors.trackNameBg;
            context.fillRect(2, yScale(trackName) + aiTasksStats.closedTrackPadding, rectWidth, aiTasksStats.trackHeight);

            this.hitTest.registerToggleTrack(2, yScale(trackName), rectWidth, aiTasksStats.trackHeight, trackName);

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

            context.moveTo(gapX, aiTasksStats.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                this.hitTest.registerGapItem(gapX - 5, aiTasksStats.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }

    private handlePreviewEtlScript(context: previewEtlScriptItemContext) {
        this.etlDefinitionsCache.showDefinitionFor(context.etlType, context.taskId, context.transformationName);
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
            const tooltipHtml = '<div class="tooltip-li">Gap start time: <div class="value">' + element.start.toLocaleTimeString() + '</div></div>' +
                '<div class="tooltip-li">Gap duration: <div class="value">' + generalUtils.formatMillis(element.durationInMillis) + '</div></div>';
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleSubscriptionPendingTooltip(itemInfo: subscriptionPendingItemInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== itemInfo) {
            let tooltipHtml = `<div class="tooltip-header"> ${itemInfo.title} </div>`;
            tooltipHtml += `<div class="tooltip-li">Duration: <div class="value">${generalUtils.formatMillis(itemInfo.duration)} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Client URI: <div class="value">${itemInfo.clientUri} </div></div>`;
            this.handleTooltip(itemInfo, x, y, tooltipHtml);
        }
    }

    private handleSubscriptionConnectionTooltip(itemInfo: subscriptionConnectionItemInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== itemInfo) {
            let tooltipHtml = `<div class="tooltip-header"> ${itemInfo.title} </div>`;
            tooltipHtml += `<div class="tooltip-li">Duration: <div class="value">${generalUtils.formatMillis(itemInfo.duration)} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Client URI: <div class="value">${itemInfo.clientUri} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Strategy: <div class="value">${itemInfo.strategy} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Number of batches acknowledged: <div class="value">${itemInfo.batchCount.toLocaleString()} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Size of all batches: <div class="value">${generalUtils.formatBytesToSize(itemInfo.totalBatchSize)} </div></div>`;

            if (itemInfo.exceptionText) {
                tooltipHtml += `<div class="tooltip-li">Message: <div class="value">${generalUtils.trimMessage(itemInfo.exceptionText, 1024)}</div>${aiTasksStats.showDetailsButton}</div>`;
            }

            this.handleTooltip(itemInfo, x, y, tooltipHtml, itemInfo.exceptionText);
        }
    }

    private handleSubscriptionErrorTooltip(itemInfo: subscriptionErrorItemInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== itemInfo) {
            let tooltipHtml = `<div class="tooltip-header">  ${itemInfo.title} </div>`;
            tooltipHtml += `<div class="tooltip-li">Client URI: <div class="value">${itemInfo.clientUri} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Strategy: <div class="value">${itemInfo.strategy} </div></div>`;
            tooltipHtml += `<div class="tooltip-li">Message: <div class="value">${generalUtils.trimMessage(itemInfo.exceptionText, 1024)} </div>${aiTasksStats.showDetailsButton}</div>`;
            this.handleTooltip(itemInfo, x, y, tooltipHtml, itemInfo.exceptionText);
        }
    }

    private handleTrackTooltip(context: trackItemContext, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== context.item) {
            const type = context.rootStats.Type;

            const isRootItem = context.rootStats.Details === context.item;

            const sectionName = this.getTaskTypeDescription(type);

            let tooltipHtml = `<div class="tooltip-header"> ${sectionName} </div>`;
            tooltipHtml += '<div class="tooltip-li">' + (isRootItem ? "Total duration" : "Duration") + ': <div class="value">' + generalUtils.formatMillis(context.item.DurationInMs) + "</div></div>";

            this.handleTooltip(context.item, x, y, tooltipHtml);
        }
    }

    private handleTooltip(element: taskOperation | timeGapInfo | performanceBaseWithCache | subscriptionErrorItemInfo | subscriptionPendingItemInfo,
                          x: number, y: number, tooltipHtml: string, details: string = undefined) {
        if (element && !this.dialogVisible) {

            this.currentDetails = details;

            this.tooltip
                .style('display', undefined)
                .html(tooltipHtml)
                .datum(element);

            const $tooltip = $(this.tooltip.node());
            const tooltipWidth = $tooltip.width();
            const tooltipHeight = $tooltip.height();

            x = Math.min(x - 80, Math.max(this.totalWidth - tooltipWidth, 0));
            y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

            this.tooltip
                .style("left", (x + 2) + "px")
                .style("top", (y + 1) + "px");

            this.tooltip
                .transition()
                .duration(250)
                .style("opacity", 1);
        } else {
            this.hideTooltip();
        }
    }

    private hideTooltip() {
        this.currentDetails = null;

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
            let importedData: exportFileFormat = JSON.parse(result);

            if (Array.isArray(importedData)) {
                // maybe we imported old format let's try to convert
                importedData = {
                    Replication: importedData as any, // we force casting here
                    Etl: [],
                    Subscription: [],
                    QueueSink: []
                }
            }

            // Data validation (currently only checking if this is an array, may do deeper validation later..
            if (!_.isObject(importedData)) {
                messagePublisher.reportError("Invalid replication stats file format", undefined, undefined);
            } else {
                this.etlData = importedData.Etl.filter(x => x.EtlType === "EmbeddingsGeneration");

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
        this.etlData.forEach(etlTaskData => {
            etlTaskData.Stats.forEach(etlStats => {
                etlStats.Performance.forEach(perfStat => {
                    liveEtlStatsWebSocketClient.fillCache(perfStat,
                        TaskUtils.default.etlTypeToStudioType(etlTaskData.EtlType, etlTaskData.EtlSubType));
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
        const etlMax = d3.max(this.etlData,
                taskData => d3.max(taskData.Stats,
                        stats => d3.max(stats.Performance,
                            (p: EtlPerformanceBaseWithCache) => p.StartedAsDate)));

        this.dateCutoff = d3.max([etlMax]);
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
            const detailedDatabaseName = DatabaseUtils.default.formatNameForFile(this.db.name, this.location);

            exportFileName = `AiTasksStats of ${detailedDatabaseName} ${moment().format("YYYY-MM-DD HH-mm")}`;
        }

        const keysToIgnore: Array<keyof performanceBaseWithCache> = ["StartedAsDate", "CompletedAsDate"];

        const filePayload: Pick<exportFileFormat, "Etl"> = {
            Etl: this.etlData,
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

    private drawTextOnStripe(context: CanvasRenderingContext2D, text: string, dx: number, xStart: number, yStart: number): void {
        context.fillStyle = this.colors.stripeTextColor;
        const textWidth = context.measureText(text).width;
        const truncatedText = graphHelper.truncText(text, textWidth, dx - 4);
        if (truncatedText) {
            context.font = "12px 'Figtree', 'Helvetica Neue', Helvetica, Arial, sans-serif";
            context.fillText(truncatedText, xStart + 2, yStart + 13, dx - 4);
        }
    }
}

export = aiTasksStats;
