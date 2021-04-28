import rbush = require("rbush");
import graphHelper = require("common/helpers/graph/graphHelper");
import gapFinder = require("common/helpers/graph/gapFinder");
import colorsManager = require("common/colorsManager");
import generalUtils = require("common/generalUtils");
import fileDownloader = require("common/fileDownloader");
import messagePublisher = require("common/messagePublisher");
import liveIOStatsWebSocketClient = require("common/liveIOStatsWebSocketClient");
import fileImporter = require("common/fileImporter");
import viewHelpers = require("common/helpers/view/viewHelpers");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndexes" | "trackItem" | "gapItem";
    arg?: any;
}

class hitTest {
    cursor = ko.observable<string>("auto");
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;
    private onToggleIndexes: () => void;
    private handleTrackTooltip: (item: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats, x: number, y: number) => void;
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
         onToggleIndexes: () => void,
         handleTrackTooltip: (item: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats, x: number, y: number) => void,
         handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
         removeTooltip: () => void) {
        this.container = container;
        this.onToggleIndexes = onToggleIndexes;
        this.handleTrackTooltip = handleTrackTooltip;
        this.handleGapTooltip = handleGapTooltip;
        this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats) {
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

    registerIndexToggle(x: number, y: number, width: number, height: number) {
        const data = {
            minX: x,
            minY: y,
            maxX: x + width,
            maxY: y + height,
            actionType: "toggleIndexes"
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

            if (item.actionType === "toggleIndexes") {
                this.onToggleIndexes();
                break;
                // Since we register broader regions, we might end up with multiple toggle items, 
                // we don't want to toggle this few times because it might result in no change at all (for even amount of matching elements)
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

        const overToggleIndexes = items.filter(x => x.actionType === "toggleIndexes").length > 0;
        this.cursor(overToggleIndexes ? "pointer" : "auto"); 

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Server.Utils.IoMetrics.IOMetricsRecentStats)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
            this.cursor("auto");
        } else {
            const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
            if (currentGapItem) {
                this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
                this.cursor("auto");
            } else {
                this.cursor(overToggleIndexes ? "pointer" : graphHelper.prefixStyle("grab"));
                this.removeTooltip();
            }
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - ioStatsGraph.brushSectionHeight,
            maxY: y - ioStatsGraph.brushSectionHeight
        });
    }
}

class legend {
    imageStr = ko.observable<string>();
    maxSize = ko.observable<number>(0);
    type: Sparrow.Server.Meters.IoMetrics.MeterType;

    sizeScale: d3.scale.Linear<number, number>;  // domain: legend pixels, range: item size
    colorScale: d3.scale.Linear<string, string>; // domain: item size,     range: item color    

    private readonly lowSizeColor: string;
    private readonly highSizeColor: string;

    static readonly imageWidth = 150;
    static readonly imageHeight = 20;
    static readonly legendArrowBorderSize = 6;

    constructor(lowSizeColor: string, highSizeColor: string, type: Sparrow.Server.Meters.IoMetrics.MeterType) {
        this.lowSizeColor = lowSizeColor;
        this.highSizeColor = highSizeColor;
        this.type = type;
    }

    setLegendScales() {
        this.sizeScale = d3.scale.linear<number>()
            .domain([-legend.legendArrowBorderSize, legend.imageWidth - legend.legendArrowBorderSize])
            .range([0, this.maxSize()]);

        this.colorScale = d3.scale.linear<string>()
            .domain([0, this.maxSize()])
            .range([this.lowSizeColor, this.highSizeColor])
            .interpolate(d3.interpolateHsl);
    }

    createLegendImage() {
        // Create legend image on a virtual canvas,
        // Will be used as an image in the dom 

        const legendCanvas = document.createElement("canvas");
        legendCanvas.width = legend.imageWidth;
        legendCanvas.height = legend.imageHeight;
        const legendContext = legendCanvas.getContext("2d");

        const widthToColorScale = d3.scale.linear<string, string>()
            .domain([0, legend.imageWidth])
            .range([this.lowSizeColor, this.highSizeColor])
            .interpolate(d3.interpolateHsl);

        legendContext.fillStyle = this.lowSizeColor;
        legendContext.fillRect(0, 0, 1, 25);

        for (let i = 0; i < legend.imageWidth; i++) {
            legendContext.fillStyle = widthToColorScale(i);
            legendContext.fillRect(i, 7, 1, legend.imageHeight);
        }

        legendContext.fillStyle = this.highSizeColor;
        legendContext.fillRect(legend.imageWidth - 1, 0, 1, 25);

        this.imageStr(legendCanvas.toDataURL());
    }
}


class ioStatsGraph {

    /* static */

    private static readonly trackHeight = 18;
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly closedTrackHeight = ioStatsGraph.closedTrackPadding + ioStatsGraph.trackHeight + ioStatsGraph.closedTrackPadding;
    private static readonly openedTrackHeight = ioStatsGraph.closedTrackHeight * 4;
    static readonly brushSectionHeight = ioStatsGraph.openedTrackHeight;

    private static readonly itemHeight = 19;
    private static readonly itemMargin = 1;
    private static readonly minItemWidth = 1;
    private static readonly charWidthApproximation = 5.2;

    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly minGapSize = 10 * 1000; // 10 seconds
    private static readonly axisHeight = 35;
    private static readonly bufferSize = 50000;

    private static readonly indexesString = "Indexes";

    private static readonly meterTypes: Array<Sparrow.Server.Meters.IoMetrics.MeterType> = [ "JournalWrite", "DataFlush", "DataSync", "Compression" ];

    /* private observables */

    private readonly supportsIndexes: boolean;
    private readonly tracksOrder: string[];
    private readonly statsNameProvider: () => string;
    private autoScroll = ko.observable<boolean>(false);
    private hasAnyData = ko.observable<boolean>(false);
    private loading: KnockoutComputed<boolean>;
    private clearSelectionVisible = ko.observable<boolean>(false);
    private importFileName = ko.observable<string>();
    private isImport = ko.observable<boolean>(false);
    private trackNames = ko.observableArray<string>();

    private searchText = ko.observable<string>("");
    private hasIndexes = ko.observable<boolean>(false);
    private isIndexesExpanded = ko.observable<boolean>(false);
    private filteredIndexesTracksNames = ko.observableArray<string>();
    private allIndexesAreFiltered = ko.observable<boolean>(false);
    indexesVisible: KnockoutComputed<boolean>;

    private legends = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, KnockoutObservable<legend>>();
    private itemSizePositions = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, KnockoutObservable<string>>();
    private itemHovered = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, KnockoutObservable<boolean>>();

    /* private */

    private readonly liveViewClientProvider: (onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void,
                                      dateCutOff?: Date) => liveIOStatsWebSocketClient;
    private liveViewClient = ko.observable<liveIOStatsWebSocketClient>();
    private data: Raven.Server.Utils.IoMetrics.IOMetricsResponse;
    private bufferIsFull = ko.observable<boolean>(false);
    private bufferUsage = ko.observable<string>("0.0");
    private dateCutoff: Date; // used to avoid showing server side cached items, after 'clear' is clicked. 
    private closedIndexesItemsCache: Array<IOMetricsRecentStatsWithCache>; 
    private totalWidth: number;
    private totalHeight: number;
    private currentYOffset = 0;
    private maxYOffset = 0;

    private gapFinder: gapFinder;
    private hitTest = new hitTest();
    private brushSection: HTMLCanvasElement; // a virtual canvas for brush section
    private brushAndZoomCallbacksDisabled = false;

    /* d3 */

    private xTickFormat = d3.time.format("%H:%M:%S");
    private canvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xBrushTimeScale: d3.time.Scale<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;
    private yScale: d3.scale.Ordinal<string, number>;
    private tooltip: d3.Selection<Raven.Server.Utils.IoMetrics.IOMetricsRecentStats | timeGapInfo>;

    /* colors */

    private scrollConfig: scrollColorConfig;
    private colors = {
        axis: undefined as string,
        axisText: undefined as string,
        gaps: undefined as string,
        trackBackground: undefined as string,
        trackNameBg: undefined as string,
        trackNameFg: undefined as string,
        openedTrackArrow: undefined as string,
        closedTrackArrow: undefined as string,
        text: undefined as string
    };

    private eventsColors: { [typeName in Sparrow.Server.Meters.IoMetrics.MeterType]: { low: string; high: string } } = {
        "Compression": {
            low: undefined as string, high: undefined as string
        },
        "DataFlush": {
            low: undefined as string, high: undefined as string
        },
        "DataSync": {
            low: undefined as string, high: undefined as string
        },
        "JournalWrite": {
            low: undefined as string, high: undefined as string
        }
    };

    constructor(statsNameProvider: () => string,
                tracksOrder: string[],
                supportsIndexes: boolean,
        liveClientProvider: (onData: (data: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => void,
                                     dateCutOff?: Date) => liveIOStatsWebSocketClient) {
        
        this.statsNameProvider = statsNameProvider;
        this.tracksOrder = tracksOrder;
        this.supportsIndexes = supportsIndexes;
        this.liveViewClientProvider = liveClientProvider;
        _.bindAll(this, "clearGraphWithConfirm");

        this.searchText.throttle(700).subscribe(() => this.filterTracks());

        ioStatsGraph.meterTypes.forEach(type => {
            this.legends.set(type, ko.observable<legend>());
            this.itemSizePositions.set(type, ko.observable<string>());
            this.itemHovered.set(type, ko.observable<boolean>(false));
        });

        this.autoScroll.subscribe(v => {
            if (v) {
                this.scrollToRight();
            } else {
                // Cancel transition (if any)
                this.brushContainer.transition();
            }
        });

        this.loading = ko.pureComputed(() => {
            const client = this.liveViewClient();
            return client ? client.loading() : true;
        });

        this.indexesVisible = ko.pureComputed(() => this.hasIndexes() && !this.allIndexesAreFiltered());
    }

    dispose() {
        if (this.liveViewClient) {
            this.cancelLiveView();
        }
    }

    init(width: number, height: number) {
        colorsManager.setup(".io-stats .main-colors", this.colors);
        colorsManager.setup(".io-stats .event-colors", this.eventsColors);
        this.scrollConfig = graphHelper.readScrollConfig();

        ioStatsGraph.meterTypes.forEach(meterType => {
            this.legends.get(meterType)(new legend(this.eventsColors[meterType].low, this.eventsColors[meterType].high, meterType));
        });

        this.tooltip = d3.select(".tooltip");
        this.totalWidth = width;
        this.totalHeight = height;

        this.initCanvas();

        this.hitTest.init(this.svg,
            () => this.onToggleIndexes(),
            (trackItem, x, y) => this.handleTrackTooltip(trackItem, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());

        this.enableLiveView();
    }

    private initLegendImages() {
        this.legends.forEach(x => x().createLegendImage());
    }

    private setLegendScales() {
        this.legends.forEach(x => x().setLegendScales());
    }
    
    private initViewData() {
        this.hasIndexes(false);

        const legendsCache = new Map<string, number>();

        // Init max size (for legend scale)
        this.legends.forEach(x => legendsCache.set(x().type, 0));

        this.data.Environments.forEach(env => {

            if (env.Type === "Index") {
                this.hasIndexes(true);
            }

            env.Files.forEach(file => {
                file.Recent.forEach(recentItem => {

                    // Calc highest batch size for each type
                    const itemValue = ioStatsGraph.extractItemValue(recentItem);
                    const currentLegendMax = legendsCache.get(recentItem.Type);
                    legendsCache.set(recentItem.Type, itemValue > currentLegendMax ? itemValue : currentLegendMax);

                    this.hasAnyData(true);
                });
            });
        });

        this.legends.forEach(legend => {
            legend().maxSize(Math.max(1, legendsCache.get(legend().type)));
        });
    }

    private initCanvas() {
        const metricsContainer = d3.select("#IOMetricsContainer");
        this.canvas = metricsContainer
            .append("canvas")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.svg = metricsContainer
            .append("svg")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight);

        this.xBrushNumericScale = d3.scale.linear<number>()
            .range([0, this.totalWidth - 1]) // subtract 1px to avoid issue with missing right stroke
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
            .attr("height", this.totalHeight - ioStatsGraph.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + ioStatsGraph.brushSectionHeight + ")")
            .call(this.zoom)
            .call(d => this.setupEvents(d));
    }

    private setupEvents(selection: d3.Selection<any>) {
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

                    this.currentYOffset = initialOffset - yDiff;
                    this.fixCurrentOffset();
                });

                selection.on("mouseup.yShift", () => selection.on("mousemove.yShift", null));
            });

        selection.on("dblclick.zoom", null);
    }

    private filterTracks() {
        const criteria = this.searchText().toLowerCase();
        this.allIndexesAreFiltered(false);

        const indexesTracks = this.data ? this.data.Environments.filter(x => x.Type === "Index") : [];
        const indexesTracksNames = indexesTracks.map(x => ioStatsGraph.findTrackName(x));

        // filteredIndexesTracksNames will be indexes tracks names that are NOT SUPPOSED TO BE SEEN ....
        this.filteredIndexesTracksNames(indexesTracksNames.filter(x => !(x.toLowerCase().includes(criteria))));

        this.allIndexesAreFiltered(this.hasAnyData() && indexesTracks.length === this.filteredIndexesTracksNames().length);

        this.updateClosedIndexesInfo();
        this.drawMainSection();
    }

    private enableLiveView() {
        let firstTime = true;

        const onDataUpdate = (mergedData: Raven.Server.Utils.IoMetrics.IOMetricsResponse) => {
            let timeRange: [Date, Date];

            if (!firstTime) {
                const timeToRemap = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
                // noinspection JSSuspiciousNameCombination
                timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
            }

            this.data = mergedData;

            this.checkBufferUsage();
            this.prepareTimeData();

            if (!firstTime) {
                const newBrush = timeRange.map(x => this.xBrushTimeScale(x)) as [number, number];
                this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
            }

            if (this.autoScroll()) {
                this.scrollToRight();
            }

            this.initViewData();
            this.setLegendScales();
            this.updateClosedIndexesInfo();

            if (firstTime) {
                this.initLegendImages();
            }

            this.draw(firstTime);

            if (firstTime) {
                firstTime = false;
            }
        };

        this.liveViewClient(this.liveViewClientProvider(onDataUpdate, this.dateCutoff));
    }

    private checkBufferUsage() {
        const dataCount = _.sumBy(this.data.Environments, env => _.sumBy(env.Files, files => files.Recent.length));

        const usage = Math.min(100, dataCount * 100.0 / ioStatsGraph.bufferSize);
        this.bufferUsage(usage.toFixed(1));

        if (dataCount > ioStatsGraph.bufferSize) {
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

    private cancelLiveView() {
        if (this.liveViewClient()) {
            this.liveViewClient().dispose();
            this.liveViewClient(null);
        }
    }

    private prepareTimeData() {
        let timeRanges = this.extractTimeRanges();

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
        }

        this.gapFinder = new gapFinder(timeRanges, ioStatsGraph.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);
    }

    private draw(resetFilteredIndexNames: boolean) {
        if (this.hasAnyData()) {

            // 0. Prepare
            this.prepareBrushSection();
            this.prepareMainSection(resetFilteredIndexNames);

            // 1. Draw the top brush section as image on the real DOM canvas
            const canvas = this.canvas.node() as HTMLCanvasElement;
            const context = canvas.getContext("2d");
            context.beginPath();
            context.clearRect(0, 0, this.totalWidth, ioStatsGraph.brushSectionHeight);
            context.drawImage(this.brushSection, 0, 0);

            // 2. Draw main (bottom) section
            this.drawMainSection();
        }
    }

    private prepareBrushSection() {

        // 1. Prepare virtual canvas element for the brush section, will not be appended to the DOM
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth;
        this.brushSection.height = ioStatsGraph.brushSectionHeight;

        const context = this.brushSection.getContext("2d");

        this.drawBrushGaps(context);

        // 2. Draw scale
        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, ioStatsGraph.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = this.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth - 1, ioStatsGraph.brushSectionHeight - 1);

        // 3. Draw accumulative data in the brush section (the top area)
        let yStartItem: number;
        const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(this.xBrushTimeScale);

        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                let lastRegisteredX1 = -1e10;
                file.Recent.forEach((recentItem: IOMetricsRecentStatsWithCache) => {

                    //       Similar to what I did in indexing performance....  For now a default high color is used                       
                    context.fillStyle = this.calcItemColor(recentItem.Type);

                    switch (recentItem.Type) {
                        case "JournalWrite":
                        case "Compression":
                            yStartItem = ioStatsGraph.closedTrackHeight;
                            break;
                        case "DataFlush":
                            yStartItem = ioStatsGraph.closedTrackHeight * 2;
                            break;
                        case "DataSync":
                            yStartItem = ioStatsGraph.closedTrackHeight * 3;
                            break;
                    }

                    // 4. Draw item on canvas
                    const x1 = this.xBrushTimeScale(recentItem.StartedAsDate);
                    let dx = extentFunc(recentItem.Duration);

                    let closeToPrevious = dx < 0.5 && x1 < lastRegisteredX1 + 0.5;
                    if (!closeToPrevious) {
                        lastRegisteredX1 = x1;
                        dx = dx < ioStatsGraph.minItemWidth ? ioStatsGraph.minItemWidth : dx;
                        context.fillRect(x1, yStartItem, dx, ioStatsGraph.trackHeight);
                    }
                });
            });
        });
        
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        context.beginPath();
        context.strokeStyle = this.colors.gaps;

        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];

            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, ioStatsGraph.brushSectionHeight - 2);
        }

        context.stroke();
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
                .attr("height", ioStatsGraph.brushSectionHeight - 1);
        }
    }

    private prepareMainSection(resetFilteredIndexNames: boolean) {
        this.trackNames(this.findTrackNamesWithoutCommonPrefix());
        if (resetFilteredIndexNames) {
            this.filteredIndexesTracksNames([]);
        }
    }

    private fixCurrentOffset() {
        this.currentYOffset = Math.min(Math.max(0, this.currentYOffset), this.maxYOffset);
    }

    private constructYScale() {
        let currentOffset = ioStatsGraph.axisHeight - this.currentYOffset;

        const domain = [] as Array<string>;
        const range = [] as Array<number>;
        let firstIndex = true;

        for (let trackIdx = 0; trackIdx < this.tracksOrder.length; trackIdx++) {
            const expectedType = this.tracksOrder[trackIdx];
            
            if (expectedType === "Index") {
                if (this.indexesVisible()) {
                    for (let i = 0; i < this.data.Environments.length; i++) {
                        const env = this.data.Environments[i];
                        if (env.Type === "Index") {
                            // 2.1 indexes closed
                            if (!this.isIndexesExpanded()) {
                                if (firstIndex) {
                                    domain.push(ioStatsGraph.indexesString);
                                    range.push(currentOffset);
                                    firstIndex = false;
                                }
                                domain.push(env.Path);
                                range.push(currentOffset);
                            }
                            // 2.2 indexes opened
                            else {
                                // If first index.... push the special indexes header ...
                                if (firstIndex) {
                                    domain.push(ioStatsGraph.indexesString);
                                    range.push(currentOffset);
                                    currentOffset += ioStatsGraph.closedTrackHeight + ioStatsGraph.trackMargin;
                                    firstIndex = false;
                                }
                                // Push the index path - only if not filtered out..
                                if (!this.filtered(env)) {
                                    domain.push(env.Path);
                                    range.push(currentOffset);
                                    currentOffset += ioStatsGraph.openedTrackHeight + ioStatsGraph.trackMargin;
                                }
                            }
                        }
                    }

                    if (!this.isIndexesExpanded()) {
                        currentOffset += ioStatsGraph.openedTrackHeight + ioStatsGraph.trackMargin;
                    }
                }
            } else {
                const expectedEnv = this.data.Environments.find(x => x.Type === expectedType);
                if (expectedEnv) {
                    domain.push(expectedEnv.Path);
                    range.push(currentOffset);
                    currentOffset += ioStatsGraph.openedTrackHeight + ioStatsGraph.trackMargin;
                }
            }
        }
        
        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {
        let offset = ioStatsGraph.axisHeight;

        if (this.isIndexesExpanded()) {
            offset += (ioStatsGraph.openedTrackHeight + ioStatsGraph.trackMargin) * this.data.Environments.length 
                + ioStatsGraph.closedTrackHeight + ioStatsGraph.trackMargin;
        } else {
            offset += (ioStatsGraph.openedTrackHeight + ioStatsGraph.trackMargin) * (this.data.Environments.filter(x => x.Type !== "Index").length + 1);
        }

        const extraBottomMargin = 10;
        const availableHeightForTracks = this.totalHeight - ioStatsGraph.brushSectionHeight;

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private findTrackNamesWithoutCommonPrefix(): string[] {
        const result = new Set<string>();

        this.data.Environments.forEach(track => {
            result.add(ioStatsGraph.findTrackName(track));
        });
        
        return Array.from(result);
    }
    
    private static findTrackName(env: Raven.Server.Utils.IoMetrics.IOMetricsEnvironment): string {
        if (env.Type === "Index") {
            // an idea here it to take last part after '\' or '/'
            // even when index has '/' in name disk path is converted to '_'
            // '\' in index name of forbidden
            
            return env.Path.split(/([\\/])/g).pop();
        }
        return env.Type;
    }

    private getTicks(scale: d3.time.Scale<number, number>): Date[] {
        return d3.range(ioStatsGraph.initialOffset, this.totalWidth - ioStatsGraph.step, ioStatsGraph.step)
            .map(y => scale.invert(y));
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();
            context.beginPath();

            context.setLineDash([4, 2]);
            context.strokeStyle = this.colors.axis;

            ticks.forEach((x, i) => {
                context.moveTo(ioStatsGraph.initialOffset + (i * ioStatsGraph.step) + 0.5, yStart);
                context.lineTo(ioStatsGraph.initialOffset + (i * ioStatsGraph.step) + 0.5, yEnd);
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

            context.textAlign = "left";
            context.textBaseline = "top";
            context.font = "10px Lato";
            context.fillStyle = this.colors.axisText;

            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), ioStatsGraph.initialOffset + (i * ioStatsGraph.step) + timePaddingLeft, timePaddingTop);
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

    private drawMainSection() {
        if (!this.data) {
            return;
        }

        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        // noinspection JSSuspiciousNameCombination
        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];

        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0);
        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        try {
            context.save();

            context.translate(0, ioStatsGraph.brushSectionHeight);
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - ioStatsGraph.brushSectionHeight);

            context.beginPath();
            context.rect(0, 0, this.totalWidth, this.totalHeight - ioStatsGraph.brushSectionHeight);
            context.clip();

            // Draw tracks background 
            this.drawTracksBackground(context);

            // Draw gaps   
            this.drawGaps(context, xScale);

            // Draw vertical dotted time lines & time labels in main section
            if (xScale.domain().length) {
                const ticks = this.getTicks(xScale);

                context.save();
                context.rect(0, ioStatsGraph.axisHeight - 3, this.totalWidth, this.totalHeight - ioStatsGraph.brushSectionHeight);
                context.clip();
                this.drawXaxisTimeLines(context, ticks, this.yScale(this.data.Environments[0].Path) - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }

            // Draw all other data (track name + items on track)
            context.beginPath();
            context.rect(0, ioStatsGraph.axisHeight, this.totalWidth, this.totalHeight - ioStatsGraph.brushSectionHeight);
            context.clip();

            const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);

            let hasAtLeastOneIndexTrack = false;

            for (let envIdx = 0; envIdx < this.data.Environments.length; envIdx++) {
                const env = this.data.Environments[envIdx];
                if (this.filtered(env)) {
                    continue;
                }
                const isIndexTrack = env.Type === "Index";
                if (isIndexTrack) {
                    hasAtLeastOneIndexTrack = true;
                }

                // draw all non indexes tracks or index track only when indexes track is expanded
                if (!isIndexTrack || this.isIndexesExpanded()) {
                    this.drawTrack(context, env, xScale, extentFunc, visibleTimeFrame);
                }
            }

            if (hasAtLeastOneIndexTrack && !this.isIndexesExpanded()) {
                this.drawClosedIndexesTrack(context, xScale, extentFunc, visibleTimeFrame);
            }

            graphHelper.drawScroll(context,
                { left: this.totalWidth, top: ioStatsGraph.axisHeight },
                this.currentYOffset,
                this.totalHeight - ioStatsGraph.brushSectionHeight - ioStatsGraph.axisHeight,
                this.maxYOffset ? this.maxYOffset + this.totalHeight - ioStatsGraph.brushSectionHeight - ioStatsGraph.axisHeight : 0,
                this.scrollConfig);
        } finally {
            context.restore();
        }
    }

    private drawTrack(context:CanvasRenderingContext2D, env: Raven.Server.Utils.IoMetrics.IOMetricsEnvironment,
                      xScale: d3.time.Scale<number, number>, extentFunc: (millis: number) => number,
                      visibleTimeFrame: [Date, Date]) {

        const yStart = this.yScale(env.Path);

        if (yStart - ioStatsGraph.axisHeight < -ioStatsGraph.openedTrackHeight * 2) {
            return;
        }

        if (yStart > this.totalHeight - ioStatsGraph.brushSectionHeight) {
            return;
        }

        const trackName = ioStatsGraph.findTrackName(env);

        // Draw track name
        this.drawTrackName(context, trackName, yStart);

        const yStartPerTypeCache = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, number>();
        yStartPerTypeCache.set("JournalWrite", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin);
        yStartPerTypeCache.set("Compression", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin);
        yStartPerTypeCache.set("DataFlush", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin * 2 + ioStatsGraph.itemHeight);
        yStartPerTypeCache.set("DataSync", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin * 3 + ioStatsGraph.itemHeight * 2);

        const visibleStartDateAsInt = visibleTimeFrame[0].getTime();
        const visibleEndDateAsInt = visibleTimeFrame[1].getTime();

        // Draw item in main canvas area (but only if item is inside the visible/selected area from the brush section..)
        context.save();

        for (let fileIdx = 0; fileIdx < env.Files.length; fileIdx++) {
            const file = env.Files[fileIdx];

            let lastRegisteredX1 = -1e10;

            const recentLength = file.Recent.length;
            for (let recentIdx = 0; recentIdx < recentLength; recentIdx++) {
                const recentItem = file.Recent[recentIdx] as IOMetricsRecentStatsWithCache;

                const itemStartDateAsInt = recentItem.StartedAsDate.getTime();
                if (visibleEndDateAsInt <= itemStartDateAsInt) {
                    continue;
                }

                const itemEndDateAsInt = recentItem.CompletedAsDate.getTime();
                if (itemEndDateAsInt <= visibleStartDateAsInt) {
                    continue;
                }

                //  Determine yStart for item
                const yStartItem = yStartPerTypeCache.get(recentItem.Type);

                const x1 = xScale(recentItem.StartedAsDate);
                const originalDx = extentFunc(recentItem.Duration);

                let closeToPrevious = originalDx < 0.5 && x1 < lastRegisteredX1 + 1;
                // when item is really small and it is quite close to last *registered* item, skip it
                // since we register bigger areas anyway
                if (!closeToPrevious) {
                    lastRegisteredX1 = x1;
                    const dx = originalDx < ioStatsGraph.minItemWidth ? ioStatsGraph.minItemWidth : originalDx;
                    context.fillStyle = this.calcItemColor(recentItem.Type, recentItem);
                    context.fillRect(x1, yStartItem, dx, ioStatsGraph.itemHeight);

                    // Register track item for tooltip (but not for the 'closed' indexes track)
                    this.hitTest.registerTrackItem(x1 - 2, yStartItem, dx + 2, ioStatsGraph.itemHeight, recentItem);
                }

                // dx > 15 allows to skip itemSizeText calculation when width is too low to fit
                if (originalDx > 15 && recentItem.Type !== "Compression") {
                    // Calc text size:
                    const itemSizeText = generalUtils.formatBytesToSize(recentItem.Size);
                    if (originalDx > itemSizeText.length * ioStatsGraph.charWidthApproximation) {
                        context.fillStyle = this.colors.text;
                        context.textAlign = "center";
                        context.font = "bold 10px Lato";
                        context.fillText(itemSizeText, x1 + originalDx / 2, yStartItem + ioStatsGraph.trackHeight / 2 + 4);
                    }
                }
            }
        }

        context.restore();
    }

    private drawClosedIndexesTrack(context:CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>,
                                   extentFunc: (millis: number) => number, visibleTimeFrame: [Date, Date]) {
        const items = this.closedIndexesItemsCache;
        const yStart = this.yScale(ioStatsGraph.indexesString);

        // Draw track name
        this.drawTrackName(context, ioStatsGraph.indexesString, yStart);

        const yStartPerTypeCache = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, number>();
        yStartPerTypeCache.set("JournalWrite", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin);
        yStartPerTypeCache.set("Compression", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin);
        yStartPerTypeCache.set("DataFlush", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin * 2 + ioStatsGraph.itemHeight);
        yStartPerTypeCache.set("DataSync", yStart + ioStatsGraph.closedTrackHeight + ioStatsGraph.itemMargin * 3 + ioStatsGraph.itemHeight * 2);

        const visibleStartDateAsInt = visibleTimeFrame[0].getTime();
        const visibleEndDateAsInt = visibleTimeFrame[1].getTime();

        // Draw item in main canvas area (but only if item is inside the visible/selected area from the brush section..)
        context.save();

        let lastRegisteredX1 = -1e10;

        for (let itemIdx = 0; itemIdx < items.length; itemIdx++) {
            const recentItem = items[itemIdx];

            const itemStartDateAsInt = recentItem.StartedAsDate.getTime();
            if (visibleEndDateAsInt <= itemStartDateAsInt) {
                continue;
            }

            const itemEndDateAsInt = recentItem.CompletedAsDate.getTime();
            if (itemEndDateAsInt <= visibleStartDateAsInt) {
                continue;
            }

            // Determine yStart for item
            const yStartItem = yStartPerTypeCache.get(recentItem.Type);

            const x1 = xScale(recentItem.StartedAsDate);
            const originalDx = extentFunc(recentItem.Duration);

            if (lastRegisteredX1 > x1) {
                // looks like we started new track
                lastRegisteredX1 = -1e10;
            }

            let closeToPrevious = originalDx < 0.5 && x1 < lastRegisteredX1 + 1;
            // when item is really small and it is quite close to last *registered* item, skip it
            // since we register bigger areas anyway
            if (!closeToPrevious) {
                lastRegisteredX1 = x1;
                const dx = originalDx < ioStatsGraph.minItemWidth ? ioStatsGraph.minItemWidth : originalDx;
                context.fillStyle = this.calcItemColor(recentItem.Type);
                context.fillRect(x1, yStartItem, dx, ioStatsGraph.itemHeight);

                // On the closed index track: 
                // Register toggle, so that indexes details will open
                this.hitTest.registerIndexToggle(x1 - 5, yStartItem, dx + 5, ioStatsGraph.itemHeight);
            }
        }

        context.restore();
    }

    /**
     * Rebuild closed track cache
     */
    private updateClosedIndexesInfo() {
        if (!this.data) {
            return;
        }
        const indexesItemsStartEnds = new Map<Sparrow.Server.Meters.IoMetrics.MeterType, Array<[Date, Date]>>();
        ioStatsGraph.meterTypes.forEach(type => indexesItemsStartEnds.set(type, []));

        const closedIndexesItemsCache = [] as Array<IOMetricsRecentStatsWithCache>;

        for (let envIdx = 0; envIdx < this.data.Environments.length; envIdx++) {
            const env = this.data.Environments[envIdx];
            if (this.filtered(env)) {
                continue;
            }
            const isIndexTrack = env.Type === "Index";
            if (isIndexTrack) {
                for (let fileIdx = 0; fileIdx < env.Files.length; fileIdx++) {
                    const file = env.Files[fileIdx];
                    for (let recentIdx = 0; recentIdx < file.Recent.length; recentIdx++) {
                        const recentItem = file.Recent[recentIdx] as IOMetricsRecentStatsWithCache;
                        closedIndexesItemsCache.push(recentItem);
                    }
                }
            }
        }

        this.closedIndexesItemsCache = closedIndexesItemsCache;
    }

    private filtered(env: Raven.Server.Utils.IoMetrics.IOMetricsEnvironment): boolean {
        return _.includes(this.filteredIndexesTracksNames(), ioStatsGraph.findTrackName(env));
    }

    private drawTracksBackground(context: CanvasRenderingContext2D) {
        context.save();

        context.beginPath();
        context.rect(0, ioStatsGraph.axisHeight, this.totalWidth, this.totalHeight - ioStatsGraph.brushSectionHeight);
        context.clip();

        context.fillStyle = this.colors.trackBackground;
        this.data.Environments.forEach(env => {
            if (!this.filtered(env)) {
                context.fillRect(0, this.yScale(env.Path), this.totalWidth, ioStatsGraph.openedTrackHeight);
            }
        });

        // The special case...draw the additional index heading when in expanded state
        if (this.isIndexesExpanded()) {
            context.fillRect(0, this.yScale(ioStatsGraph.indexesString), context.measureText(ioStatsGraph.indexesString).width + 30, ioStatsGraph.closedTrackHeight);
            this.drawTrackName(context, ioStatsGraph.indexesString, this.yScale(ioStatsGraph.indexesString));
        }

        context.restore();
    }

    private drawTrackName(context: CanvasRenderingContext2D, trackName: string, yStart: number) {
        const yTextShift = 14.5;
        const xTextShift = 0.5;
        let xTextStart = 5;
        let rectWidth: number;
        let addedWidth = 8;
        let drawArrow = false;
        let skipDrawing = false;

        const isIndexTrack = trackName.startsWith(ioStatsGraph.indexesString);

        // 1. Draw background color for track name - first check if track is an 'index' track
        if (isIndexTrack) {
            xTextStart = 15;
            addedWidth = 18;

            trackName = trackName.substring(ioStatsGraph.indexesString.length + 1);

            // 1.1 The first indexes track has the track name of: 'Indexes' (both when opened or closed..)
            if ((trackName === "") || (!this.isIndexesExpanded())) {
                trackName = ioStatsGraph.indexesString;
                addedWidth = 23;
                drawArrow = true;
                skipDrawing = this.allIndexesAreFiltered();
            }
        }

        if (!skipDrawing) {
            context.font = "12px Lato"; // Define font before using measureText()...
            rectWidth = context.measureText(trackName).width + addedWidth;
            context.fillStyle = this.colors.trackNameBg;

            if (!_.includes(this.filteredIndexesTracksNames(), trackName)) {
                context.fillRect(2, yStart + ioStatsGraph.closedTrackPadding, rectWidth, ioStatsGraph.trackHeight);
            }

            // 2. Draw arrow only for indexes track
            if (drawArrow) {
                context.fillStyle = this.isIndexesExpanded() ? this.colors.openedTrackArrow : this.colors.closedTrackArrow;
                graphHelper.drawArrow(context, 5, yStart + 6, !this.isIndexesExpanded());
                this.hitTest.registerIndexToggle(2, yStart + ioStatsGraph.closedTrackPadding, rectWidth, ioStatsGraph.trackHeight);
            }

            // 3. Draw track name (if not filtered out..)                
            context.fillStyle = this.colors.trackNameFg;
            context.beginPath();

            if (!_.includes(this.filteredIndexesTracksNames(), trackName)) {
                context.fillText(trackName, xTextStart + xTextShift, yStart + yTextShift);
            }
        }
    }

    private drawGaps(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        // xScale.range has screen pixels locations of Activity periods
        // xScale.domain has Start & End times of Activity periods

        const range = xScale.range();
        const domain = xScale.domain();

        context.beginPath();
        context.strokeStyle = this.colors.gaps;

        for (let i = 1; i < range.length - 1; i += 2) {
            const gapX = Math.floor(range[i]) + 0.5;

            context.moveTo(gapX, ioStatsGraph.axisHeight);
            context.lineTo(gapX, this.totalHeight);

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = domain[i];
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                // Register gap for tooltip 
                this.hitTest.registerGapItem(gapX - 5, ioStatsGraph.axisHeight, 10, this.totalHeight,
                    { durationInMillis: gapInfo.durationInMillis, start: gapInfo.start });
            }
        }

        context.stroke();
    }

    private onToggleIndexes() {
        this.isIndexesExpanded.toggle();
        this.drawMainSection();
    }

    expandIndexes() {
        this.isIndexesExpanded(true);
        this.drawMainSection();
    }

    collapseIndexes() {
        this.isIndexesExpanded(false);
        this.drawMainSection();
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
            const importedData: Raven.Server.Utils.IoMetrics.IOMetricsResponse = JSON.parse(result);
            
            const hasEnvironments = importedData.hasOwnProperty('Environments');
            
            // Check if data is an IOStats json data..
            if (!hasEnvironments) {
                messagePublisher.reportError("Invalid IO Stats file format", undefined, undefined);
            } else {
                const importedEnvTypes = Array.from(new Set<string>(importedData.Environments.map(x => x.Type)));
                if (importedEnvTypes.some(x => !_.includes(this.tracksOrder, x))) {
                    messagePublisher.reportError("Invalid IO Stats file format", undefined, undefined);
                } else {
                    if (this.hasAnyData()) {
                        this.resetGraphData();
                    }
                    this.data = importedData;
                    this.fillCache();
                    this.prepareTimeData();
                    this.initViewData();
                    this.setLegendScales();
                    this.updateClosedIndexesInfo();
                    this.draw(true);
                    this.isImport(true);
                }
            }
        } catch (e) {
            messagePublisher.reportError("Failed to parse json data", undefined, undefined);
        }
    }

    private fillCache() {
        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.Recent.forEach(stat => {
                    liveIOStatsWebSocketClient.fillCache(stat);
                });
            });
        });
    }

    clearGraphWithConfirm() {
        viewHelpers.confirmationMessage("Clear graph data", "Do you want to discard all collected IO statistics?")
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
        this.dateCutoff = d3.max(this.data.Environments,
            env => d3.max(env.Files,
                file => d3.max(file.Recent,
                    (r: IOMetricsRecentStatsWithCache) => r.StartedAsDate)));
    }

    closeImport() {
        this.dateCutoff = null;
        this.isImport(false);
        this.clearGraph();
    }

    private resetGraphData() {
        this.data = null;
        this.searchText("");
        this.hasAnyData(false);
        this.allIndexesAreFiltered(false);
        this.bufferUsage("0.0");
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());
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
            exportFileName = `IOStats-of-${this.statsNameProvider()}-${moment().format("YYYY-MM-DD-HH-mm")}`;
        }

        const keysToIgnore: Array<keyof IOMetricsRecentStatsWithCache> = ["StartedAsDate", "CompletedAsDate"];

        fileDownloader.downloadAsJson(this.data, exportFileName + ".json", exportFileName, (key, value) => {
            if (_.includes(keysToIgnore, key)) {
                return undefined;
            }
            return value;
        });
    }
    
    private calcItemColor(type: Sparrow.Server.Meters.IoMetrics.MeterType, recentItem?: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats): string {
        if (recentItem) {
            return this.legends.get(type)().colorScale(ioStatsGraph.extractItemValue(recentItem));
        } else {
            return this.eventsColors[type].high;
        }
    }

    private static extractItemValue(item: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats) {
        return item.Type === "Compression" ? (item as Raven.Server.Utils.IoMetrics.IOMetricsRecentStatsAdditionalTypes).CompressionRatio : item.Size;
    }

    private extractTimeRanges(): Array<[Date, Date]>{
        const result = [] as Array<[Date, Date]>;
        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.Recent.forEach((recentItem: IOMetricsRecentStatsWithCache) => {
                    // Get the events time ranges
                    result.push([recentItem.StartedAsDate, recentItem.CompletedAsDate]);
                });
            });
        });

        return result;
    }

    computedItemValue(legendWrapped: KnockoutObservable<legend>): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            const legend = legendWrapped();
            if (legend.type === "Compression") {
                return (legend.maxSize() * 100).toFixed(2) + '%';
            } else {
                return generalUtils.formatBytesToSize(legend.maxSize());
            }
        });
    }

    /*
    * The following methods are called by hitTest class on mouse move    
    */

    private handleGapTooltip(element: timeGapInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = '<div class="tooltip-li">Gap start time: <span class="value">' + (element).start.toLocaleTimeString() + '</span></div>'
                + '<div class="tooltip-li">Gap duration:  <span class="value">' + generalUtils.formatMillis((element).durationInMillis) + '</span></div>';
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private static getMeterTypeFriendlyName(type: Sparrow.Server.Meters.IoMetrics.MeterType) {
        switch (type) {
            case "JournalWrite":
                return "Journal Write";
            case "DataFlush":
                return "Voron Data Flush";
            case "DataSync":
                return "Voron Data Sync";
            case "Compression":
                return "Compression Ratio";
        }
    }

    private handleTrackTooltip(element: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        // 1. Show item size position in the legend (in addition to showing the tooltip)
        this.itemHovered.forEach(x => x(false));

        this.itemHovered.get(element.Type)(true);

        this.itemSizePositions.get(element.Type)(this.legends.get(element.Type)().sizeScale.invert(ioStatsGraph.extractItemValue(element)).toString() + "px");

        // 2. Show tooltip
        if (currentDatum !== element) {
            const typeString = ioStatsGraph.getMeterTypeFriendlyName(element.Type);

            let tooltipHtml = `<div class="tooltip-header"> ${typeString} </div>`;
            const duration = (element.Duration === 0) ? "0" : generalUtils.formatMillis((element).Duration);
            tooltipHtml += `<div class="tooltip-li">Duration: <span class="value">${duration}</span></div>`;

            if (element.Type !== "Compression") {
                tooltipHtml += `<div class="tooltip-li">Size: <span class="value">${generalUtils.formatBytesToSize(element.Size)}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Size (bytes): <span class="value">${element.Size.toLocaleString()}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Allocated Size: <span class="value">${generalUtils.formatBytesToSize(element.FileSize)}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Allocated Size (bytes): <span class="value">${element.FileSize.toLocaleString()}</span></div>`;

                const speed = element.Duration ? generalUtils.formatBytesToSize(element.Size / element.Duration * 1000) + " / s" : "unknown";
                tooltipHtml += `<div class="tooltip-li">Speed: <span class="value">${speed}</span></div>`;

            } else {
                const compressionElement = element as Raven.Server.Utils.IoMetrics.IOMetricsRecentStatsAdditionalTypes;
                tooltipHtml += `<div class="tooltip-li">Original Size: <span class="value">${generalUtils.formatBytesToSize(compressionElement.OriginalSize)}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Original Size (bytes): <span class="value">${compressionElement.OriginalSize.toLocaleString()}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Compressed Size: <span class="value">${generalUtils.formatBytesToSize(compressionElement.CompressedSize)}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Compressed Size (bytes): <span class="value">${compressionElement.CompressedSize.toLocaleString()}</span></div>`;
                tooltipHtml += `<div class="tooltip-li">Compression Ratio: <span class="value">${(compressionElement.CompressionRatio * 100).toFixed(2)}%</span></div>`;

                const compressionSpeed = element.Duration ? generalUtils.formatBytesToSize(compressionElement.OriginalSize / element.Duration * 1000) + " / s" : "unknown";
                tooltipHtml += `<div class="tooltip-li">Compression speed: <span class="value">${compressionSpeed}</span></div>`;
            }

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleTooltip(element: Raven.Server.Utils.IoMetrics.IOMetricsRecentStats | timeGapInfo, x: number, y: number, tooltipHtml: string) {
        if (element) {
            this.tooltip
                .style("display", undefined)
                .html(tooltipHtml)
                .datum(element);
            
            const $tooltip = $(this.tooltip.node());
            const tooltipWidth = $tooltip.width();
            const tooltipHeight = $tooltip.height();

            x = Math.min(x, Math.max(this.totalWidth - tooltipWidth, 0));
            y = Math.min(y, Math.max(this.totalHeight - tooltipHeight, 0));

            this.tooltip
                .style("left", (x + 10) + "px")
                .style("top", (y + 10) + "px")
            
            this.tooltip
                .transition()
                .duration(250)
                .style("opacity", 1);
        } else {
            this.hideTooltip();
        }
    }

    private hideTooltip() {
        this.tooltip
            .transition()
            .duration(250)
            .style("opacity", 0);

        this.tooltip.datum(null);

        // No need to show arrow position in legend any more..
        this.itemHovered.forEach(x => x(false));
    }

    clearBrush() {
        this.autoScroll(false);
        this.brush.clear();
        this.brushContainer.call(this.brush);

        this.onBrush();
    }
}

export = ioStatsGraph;
