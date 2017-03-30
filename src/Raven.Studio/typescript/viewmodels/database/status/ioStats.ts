import d3 = require("d3");
import rbush = require("rbush");
import generalUtils = require("common/generalUtils");
import fileDownloader = require("common/fileDownloader");
import viewModelBase = require("viewmodels/viewModelBase");
import gapFinder = require("common/helpers/graph/gapFinder");
import messagePublisher = require("common/messagePublisher");
import graphHelper = require("common/helpers/graph/graphHelper");
import liveIOStatsWebSocketClient = require("common/liveIOStatsWebSocketClient");

type rTreeLeaf = {
    minX: number;
    minY: number;
    maxX: number;
    maxY: number;
    actionType: "toggleIndexes" | "trackItem" | "closedTrackItem" | "gapItem";
    arg: any;
}

class hitTest {
    cursor = ko.observable<string>("auto");   
    private rTree = rbush<rTreeLeaf>();
    private container: d3.Selection<any>;    
    private onToggleIndexes: () => void;
    private handleTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void;   
    private handleClosedTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void;
    private handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void;
    private removeTooltip: () => void;

    reset() {
        this.rTree.clear();
    }

    init(container: d3.Selection<any>,
        onToggleIndexes: () => void,
        handleTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void,
        handleClosedTrackTooltip: (item: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) => void,
        handleGapTooltip: (item: timeGapInfo, x: number, y: number) => void,
        removeTooltip: () => void) {
            this.container = container;
            this.onToggleIndexes = onToggleIndexes;
            this.handleTrackTooltip = handleTrackTooltip;
            this.handleClosedTrackTooltip = handleClosedTrackTooltip;
            this.handleGapTooltip = handleGapTooltip;
            this.removeTooltip = removeTooltip;
    }

    registerTrackItem(x: number, y: number, width: number, height: number, element: Raven.Server.Documents.Handlers.IOMetricsRecentStats) {
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

    registerClosedTrackItem(x: number, y: number, width: number, height: number, element: Raven.Server.Documents.Handlers.IOMetricsRecentStats) {
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

    onMouseMove() {
        const clickLocation = d3.mouse(this.container.node());
        const items = this.findItems(clickLocation[0], clickLocation[1]);

        const overToggleIndexes = items.filter(x => x.actionType === "toggleIndexes").length > 0;
        this.cursor(overToggleIndexes ? "pointer" : "auto");

        const currentItem = items.filter(x => x.actionType === "trackItem").map(x => x.arg as Raven.Server.Documents.Handlers.IOMetricsRecentStats)[0];
        if (currentItem) {
            this.handleTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);          
        }
        else {
            const currentItem = items.filter(x => x.actionType === "closedTrackItem").map(x => x.arg as Raven.Server.Documents.Handlers.IOMetricsRecentStats)[0];
            if (currentItem) {
                this.handleClosedTrackTooltip(currentItem, clickLocation[0], clickLocation[1]);
            }
            else {
                const currentGapItem = items.filter(x => x.actionType === "gapItem").map(x => x.arg as timeGapInfo)[0];
                if (currentGapItem) {
                    this.handleGapTooltip(currentGapItem, clickLocation[0], clickLocation[1]);
                }
                else {
                    this.removeTooltip();
                }
            }           
        }
    }

    private findItems(x: number, y: number): Array<rTreeLeaf> {
        return this.rTree.search({
            minX: x,
            maxX: x,
            minY: y - ioStats.brushSectionHeight,
            maxY: y - ioStats.brushSectionHeight
        });
    }
}

class legend {
    imageStr = ko.observable<string>();
    maxSize = ko.observable<number>(0);   

    sizeScale: d3.scale.Linear<number, number>;  // domain: legend pixels, range: item size
    colorScale: d3.scale.Linear<string, string>; // domain: item size,     range: item color    

    private lowSizeColor: string;
    private highSizeColor: string;
        
    static readonly imageWidth = 150;
    static readonly imageHeight = 20;   
    static readonly legendArrowBorderSize = 6;

    constructor(lowSizeColor: string, highSizeColor: string) {        
        this.lowSizeColor = lowSizeColor;
        this.highSizeColor = highSizeColor;        
    }  

    setLegendScales() {
        this.sizeScale = d3.scale.linear<number>()
            .domain([0, legend.imageWidth - legend.legendArrowBorderSize])
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

class ioStats extends viewModelBase {

    /* static */

    static readonly colors = {
        axis: "#546175",
        gaps: "#ca1c59",              
        trackBackground: "#2c343a",       
        trackNameBg: "rgba(57, 67, 79, 0.8)",
        trackNameFg: "#98a7b7",
        openedTrackArrow: "#ca1c59",
        closedTrackArrow: "#98a7b7"        
    }

    static readonly eventsColors = {       
        "LowSizeColorJW": "#38761d",       // JW - Journal Write
        "HighSizeColorJW": "#93c47d",
        "LowSizeColorDF": "#085394",       // DF - Data Flush
        "HighSizeColorDF": "#6fa8dc",
        "LowSizeColorDS": "#b45f06",       // DS - Data Sync
        "HighSizeColorDS": "#f6b26b"
    }
   
    private static readonly trackHeight = 18; 
    private static readonly trackMargin = 4;
    private static readonly closedTrackPadding = 2;
    private static readonly openedTrackPadding = 4;
    private static readonly closedTrackHeight = ioStats.closedTrackPadding + ioStats.trackHeight + ioStats.closedTrackPadding;
    private static readonly openedTrackHeight = ioStats.closedTrackHeight * 4;
    static readonly brushSectionHeight = ioStats.openedTrackHeight;       

    private static readonly itemHeight = 19;
    private static readonly itemMargin = 1;    
    private static readonly minItemWidth = 1;

    private static readonly initialOffset = 100;
    private static readonly step = 200;
    private static readonly minGapSize = 10 * 1000; // 10 seconds      
    private static readonly axisHeight = 35; 

    private static readonly indexesString = "Indexes";
    private static readonly documentsString = "Documents";
    private static readonly journalWriteString = "JournalWrite";  
    private static readonly dataFlushString = "DataFlush";      
    private static readonly dataSyncString = "DataSync"; 

    /* private observables */

    private autoScroll = ko.observable<boolean>(false);
    private hasAnyData = ko.observable<boolean>(false);    
    private importFileName = ko.observable<string>();   
    private isImport = ko.observable<boolean>(false);
    private trackNames = ko.observableArray<string>();

    private searchText = ko.observable<string>();
    private hasIndexes = ko.observable<boolean>(false);
    private isIndexesExpanded = ko.observable<boolean>(false);     
    private filteredIndexesTracksNames = ko.observableArray<string>();   
    private allIndexesAreFiltered = ko.observable<boolean>(false);
    private indexesVisible: KnockoutComputed<boolean>; 

    private legendJW = ko.observable<legend>();
    private legendDF = ko.observable<legend>();
    private legendDS = ko.observable<legend>(); 

    private itemSizePositionJW = ko.observable<string>(); 
    private itemSizePositionDF = ko.observable<string>(); 
    private itemSizePositionDS = ko.observable<string>(); 

    private itemHoveredJW = ko.observable<boolean>(false); 
    private itemHoveredDF = ko.observable<boolean>(false); 
    private itemHoveredDS = ko.observable<boolean>(false); 

    /* private */

    private liveViewClient: liveIOStatsWebSocketClient;
    private data: Raven.Server.Documents.Handlers.IOMetricsResponse;    
    private commonPathsPrefix: string;     
    private totalWidth: number;
    private totalHeight: number; 
    private currentYOffset = 0;
    private maxYOffset = 0;

    private gapFinder: gapFinder;   
    private hitTest = new hitTest();
    private brushSection: HTMLCanvasElement; // a virtual canvas for brush section
    private brushAndZoomCallbacksDisabled = false;    

    private indexesItemsStartEndJW: Array<[number, number]>; // Start & End times for joined duration times for closed index track items
    private indexesItemsStartEndDF: Array<[number, number]>; 
    private indexesItemsStartEndDS: Array<[number, number]>; 

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
    private tooltip: d3.Selection<Raven.Server.Documents.Handlers.IOMetricsRecentStats | timeGapInfo>; 

    constructor() {
        super();
        this.searchText.throttle(200).subscribe(() => this.filterTracks());

        this.autoScroll.subscribe(v => {
            if (v) {
                this.scrollToRight();
            } else {
                // Cancel transition (if any)
                this.brushContainer.transition();
            }
        });
    }

    activate(args: { indexName: string, database: string }): void {
        super.activate(args);        
        this.indexesVisible = ko.pureComputed(() => this.hasIndexes() && !this.allIndexesAreFiltered());    

        this.legendJW(new legend(ioStats.eventsColors.LowSizeColorJW, ioStats.eventsColors.HighSizeColorJW));
        this.legendDF(new legend(ioStats.eventsColors.LowSizeColorDF, ioStats.eventsColors.HighSizeColorDF));
        this.legendDS(new legend(ioStats.eventsColors.LowSizeColorDS, ioStats.eventsColors.HighSizeColorDS));
    }

    deactivate() {
        super.deactivate();

        if (this.liveViewClient) {
            this.cancelLiveView();
        }
    }

    compositionComplete() {
        super.compositionComplete();

        this.tooltip = d3.select(".tooltip");
        [this.totalWidth, this.totalHeight] = this.getPageHostDimenensions();
        this.totalHeight -= 50; // substract toolbar height
              
        this.initCanvas();     

        this.hitTest.init(this.svg,
            () => this.onToggleIndexes(),
            (trackItem, x, y) => this.handleTrackTooltip(trackItem, x, y),
            (closedTrackItem, x, y) => this.handleClosedTrackTooltip(closedTrackItem, x, y),
            (gapItem, x, y) => this.handleGapTooltip(gapItem, x, y),
            () => this.hideTooltip());                    
         
        this.enableLiveView();
    }

    private initLegendImages() {     
        this.legendJW().createLegendImage();
        this.legendDF().createLegendImage();
        this.legendDS().createLegendImage();
    }  
  
    private setLegendScales() {
        this.legendJW().setLegendScales();
        this.legendDF().setLegendScales();
        this.legendDS().setLegendScales();               
    }

    private initViewData() {        
        this.hasIndexes(false);

        // 1. Find common paths prefix
        this.commonPathsPrefix = this.findPrefix(this.data.Environments.map(env => env.Path));

        // 1.1 Init max size (for legend scale)       
        this.legendJW().maxSize(0);
        this.legendDF().maxSize(0);
        this.legendDS().maxSize(0);

        // 2. Loop on info from EndPoint        
        this.data.Environments.forEach(env => {           

            // 2.0 Set the track name for the database path
            let trackName = env.Path.substring(this.commonPathsPrefix.length);           

            // 2.1 Check if indexes exist        
            if (env.Path.substring(this.commonPathsPrefix.length).startsWith(ioStats.indexesString)) {              
                this.hasIndexes(true);
            } 

            // 2.2 Retrieve data for legend
            env.Files.forEach(file => {
                file.Recent.forEach(recentItem => {
                   
                    // 2.3 Calc highest batch size for each type
                    if (recentItem.Type === ioStats.journalWriteString) {
                        this.legendJW().maxSize(recentItem.Size > this.legendJW().maxSize() ? recentItem.Size : this.legendJW().maxSize());
                    }
                    if (recentItem.Type === ioStats.dataFlushString) {
                        this.legendDF().maxSize(recentItem.Size > this.legendDF().maxSize() ? recentItem.Size : this.legendDF().maxSize());

                    }
                    if (recentItem.Type === ioStats.dataSyncString) {
                        this.legendDS().maxSize(recentItem.Size > this.legendDS().maxSize() ? recentItem.Size : this.legendDS().maxSize());
                    }

                    this.hasAnyData(true);
                });
            });
        });
   
        this.legendJW().maxSize(this.legendJW().maxSize() === 0 ? 1 : this.legendJW().maxSize());
        this.legendDF().maxSize(this.legendDF().maxSize() === 0 ? 1 : this.legendDF().maxSize());
        this.legendDS().maxSize(this.legendDS().maxSize() === 0 ? 1 : this.legendDS().maxSize());         
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
            .range([0, this.totalWidth - 1]) // substract 1px to avoid issue with missing right stroke
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
            .attr("height", this.totalHeight - ioStats.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + ioStats.brushSectionHeight + ")")
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
            .on("mousedown.tip", () => selection.on("mousemove.tip", null))
            .on("mouseup.tip", () => selection.on("mousemove.tip", onMove));

        selection
            .on("mousedown.live", () => {
                if (this.liveViewClient) {
                    this.liveViewClient.pauseUpdates();
                }
            });
        selection
            .on("mouseup.live", () => {
                if (this.liveViewClient) {
                    this.liveViewClient.resumeUpdates();
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

    private filterTracks() {
        const criteria = this.searchText().toLowerCase();
        this.allIndexesAreFiltered(false);

        const indexesTracks = this.data.Environments.filter((x) => {               
            let temp = x.Path.substring(this.commonPathsPrefix.length);
            return temp.startsWith(ioStats.indexesString);
        });                 
       
        const indexesTracksNames = indexesTracks.map(x => x.Path.substring(this.commonPathsPrefix.length + 1 + ioStats.indexesString.length));

        // filteredIndexesTracksNames will be indexes tracks names that are NOT SUPPOSED TO BE SEEN ....
        this.filteredIndexesTracksNames(indexesTracksNames.filter(x => !(x.toLowerCase().includes(criteria))));       

        this.allIndexesAreFiltered(indexesTracks.length === this.filteredIndexesTracksNames().length);     

        this.drawMainSection();
    }

    private enableLiveView() {
        let firstTime = true;

        const onDataUpdate = (mergedData: Raven.Server.Documents.Handlers.IOMetricsResponse) => {
            let timeRange: [Date, Date];
            if (!firstTime) {
                const timeToRemap = this.brush.empty() ? this.xBrushNumericScale.domain() as [number, number] : this.brush.extent() as [number, number];
                timeRange = timeToRemap.map(x => this.xBrushTimeScale.invert(x));
            }

            this.data = mergedData;        
            this.prepareTimeData();

            if (!firstTime) {
                const newBrush: [number, number] = timeRange.map(x => this.xBrushTimeScale(x));
                this.setZoomAndBrush(newBrush, brush => brush.extent(newBrush));
            }

            if (this.autoScroll()) {
                this.scrollToRight();
            }

            this.initViewData();
            this.setLegendScales();

            if (firstTime) {
                this.initLegendImages();
            }
           
            this.draw(firstTime);

            if (firstTime) {
                firstTime = false;
            }
        };

        this.liveViewClient = new liveIOStatsWebSocketClient(this.activeDatabase(), onDataUpdate);
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
        if (this.liveViewClient) {
            this.liveViewClient.dispose();
            this.liveViewClient = null;
        }
    }   

    private prepareTimeData() {
        let timeRanges = this.extractTimeRanges();

        if (timeRanges.length === 0) {
            // no data - create fake scale
            timeRanges = [[new Date(), new Date()]];
        }

        this.gapFinder = new gapFinder(timeRanges, ioStats.minGapSize);
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
            context.clearRect(0, 0, this.totalWidth, ioStats.brushSectionHeight);
            context.drawImage(this.brushSection, 0, 0);

            // 2. Draw main (bottom) section
            this.drawMainSection();
        }        
    }

    private prepareBrushSection() {

        // 1. Prepare virtual canvas element for the brush section, will not be appended to the DOM
        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth;
        this.brushSection.height = ioStats.brushSectionHeight;
       
        const timeRanges = this.extractTimeRanges();
        this.gapFinder = new gapFinder(timeRanges, ioStats.minGapSize);
        this.xBrushTimeScale = this.gapFinder.createScale(this.totalWidth, 0);

        const context = this.brushSection.getContext("2d");

        // 2. Draw scale
        const ticks = this.getTicks(this.xBrushTimeScale);
        this.drawXaxisTimeLines(context, ticks, 0, ioStats.brushSectionHeight);
        this.drawXaxisTimeLabels(context, ticks, 5, 5);

        context.strokeStyle = ioStats.colors.axis;
        context.strokeRect(0.5, 0.5, this.totalWidth - 1, ioStats.brushSectionHeight - 1);

        // 3. Draw accumulative data in the brush section (the top area)
        let yStartItem: number;      
        const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(this.xBrushTimeScale);

        this.data.Environments.forEach(env => {
            env.Files.forEach(file => {
                file.Recent.forEach((recentItem: IOMetricsRecentStatsWithCache) => {

                    // TODO: Maybe create algorithm to calculate the exact color to be painted in the brush section for the Accumulated Data,
                    //       Similar to what I did in indexing performance....  For now a default high color is used                       
                    context.fillStyle = this.calcItemColor(recentItem, false);

                    switch (recentItem.Type) {
                        case ioStats.journalWriteString:
                            yStartItem = ioStats.closedTrackHeight;
                            break;
                        case ioStats.dataFlushString:
                            yStartItem = ioStats.closedTrackHeight * 2;
                            break;
                        case ioStats.dataSyncString:
                            yStartItem = ioStats.closedTrackHeight * 3;       
                            break;                        
                    }
                   
                    // 4. Draw item on canvas
                    const x1 = this.xBrushTimeScale(recentItem.StartedAsDate);
                    let dx = extentFunc(recentItem.Duration);
                    dx = dx < ioStats.minItemWidth ? ioStats.minItemWidth : dx;                   
                    context.fillRect(x1, yStartItem, dx, ioStats.trackHeight);                 
                });
            });
        });

        this.drawBrushGaps(context);
        this.prepareBrush();
    }

    private drawBrushGaps(context: CanvasRenderingContext2D) {
        context.beginPath();
        context.strokeStyle = ioStats.colors.gaps;

        for (let i = 0; i < this.gapFinder.gapsPositions.length; i++) {
            const gap = this.gapFinder.gapsPositions[i];
           
            const gapX = this.xBrushTimeScale(gap.start);
            context.moveTo(gapX, 1);
            context.lineTo(gapX, ioStats.brushSectionHeight - 2);            
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
                .attr("y", 0)
                .attr("height", ioStats.brushSectionHeight - 1);
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
        let currentOffset = ioStats.axisHeight - this.currentYOffset;  
      
        let domain = [] as Array<string>;
        let range = [] as Array<number>;
        let firstIndex = true;        

        // TODO: Maybe refactor this method so it can handle any incoming number of environments,
        // But, as discussed, this will be left out for now in order to avoid extra string comparisons

        // 1. Database main path
        domain.push(this.data.Environments[0].Path);
        range.push(currentOffset);
        currentOffset += ioStats.openedTrackHeight + ioStats.trackMargin;

        // 2. We want indexes to show in second track even though they are last in the endpoint info..       
        if (this.indexesVisible()) {
            for (let i = 3; i < this.data.Environments.length; i++) {

                // 2.1 indexes closed
                if (!this.isIndexesExpanded()) {
                    if (firstIndex) {
                        domain.push(ioStats.indexesString);
                        range.push(currentOffset);
                        firstIndex = false;
                    }
                    domain.push(this.data.Environments[i].Path);
                    range.push(currentOffset);
                }
                // 2.2 indexes opened
                else {
                    // If first index.... push the special indexes header ...
                    if (firstIndex) {
                        domain.push(ioStats.indexesString);
                        range.push(currentOffset);
                        currentOffset += ioStats.closedTrackHeight + ioStats.trackMargin;
                        firstIndex = false;
                    }
                    // Push the index path - only if not filtered out..
                    if (!this.filtered(this.data.Environments[i].Path)) {
                        domain.push(this.data.Environments[i].Path);
                        range.push(currentOffset);
                        currentOffset += ioStats.openedTrackHeight + ioStats.trackMargin;
                    }
                }
            }

            if (!this.isIndexesExpanded()) {
                currentOffset += ioStats.openedTrackHeight + ioStats.trackMargin;
            }
        }        

        // 3. Subscriptions path
        domain.push(this.data.Environments[1].Path);
        range.push(currentOffset);
        currentOffset += ioStats.openedTrackHeight + ioStats.trackMargin;

        // 4. Configuration path
        domain.push(this.data.Environments[2].Path);
        range.push(currentOffset);
       
        this.yScale = d3.scale.ordinal<string, number>()
            .domain(domain)
            .range(range);
    }

    private calcMaxYOffset() {    
        let offset = ioStats.axisHeight;       

        if (this.isIndexesExpanded()) {
            offset += ioStats.openedTrackHeight * this.data.Environments.length + ioStats.closedTrackHeight;
        }
        else {                      
            offset += ioStats.openedTrackHeight * 4; // * 4 because I have 4 tracks: Data|Indexes|Subscriptions|Configurations
        }        

        const extraBottomMargin = 100;
        const availableHeightForTracks = this.totalHeight - ioStats.brushSectionHeight;        

        this.maxYOffset = Math.max(offset + extraBottomMargin - availableHeightForTracks, 0);
    }

    private findTrackNamesWithoutCommonPrefix(): string[] {
        const result = new Set<string>();              

        this.data.Environments.forEach(track => {                  
            let trackName = track.Path.substring(this.commonPathsPrefix.length);           
            result.add(trackName);
        });

        return Array.from(result);
    }

    private getTicks(scale: d3.time.Scale<number, number>): Date[] {       
        return d3.range(ioStats.initialOffset, this.totalWidth - ioStats.step, ioStats.step)
                 .map(y => scale.invert(y));   
    }

    private drawXaxisTimeLines(context: CanvasRenderingContext2D, ticks: Date[], yStart: number, yEnd: number) {
        try {
            context.save();                                      
            context.beginPath();

            context.setLineDash([4, 2]);           
            context.strokeStyle = ioStats.colors.axis;    

            ticks.forEach((x, i) => {
                context.moveTo(ioStats.initialOffset + (i * ioStats.step) + 0.5, yStart);
                context.lineTo(ioStats.initialOffset + (i * ioStats.step) + 0.5, yEnd);
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
            context.fillStyle = ioStats.colors.axis;
           
            ticks.forEach((x, i) => {
                context.fillText(this.xTickFormat(x), ioStats.initialOffset + (i * ioStats.step) + timePaddingLeft, timePaddingTop);
            });
        }
        finally {
            context.restore();
        }
    }

    private onZoom() {
        this.autoScroll(false);

        if (!this.brushAndZoomCallbacksDisabled) {
            this.brush.extent(this.xNumericScale.domain() as [number, number]);
            this.brushContainer
                .call(this.brush);

            this.drawMainSection();
        }
    }

    private onBrush() {
        if (!this.brushAndZoomCallbacksDisabled) {
            this.xNumericScale.domain((this.brush.empty() ? this.xBrushNumericScale.domain() : this.brush.extent()) as [number, number]);
            this.zoom.x(this.xNumericScale);
            this.drawMainSection();
        }
    }

    private drawMainSection() {
        this.hitTest.reset();
        this.calcMaxYOffset();
        this.fixCurrentOffset();
        this.constructYScale();

        const visibleTimeFrame = this.xNumericScale.domain().map(x => this.xBrushTimeScale.invert(x)) as [Date, Date];
        const visibleStartDateAsInt = visibleTimeFrame[0].getTime();
        const visibleEndDateAsInt = visibleTimeFrame[1].getTime();

        const xScale = this.gapFinder.trimmedScale(visibleTimeFrame, this.totalWidth, 0); 
        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        let yStartItem: number;
        let firstIndexTrack = true;

        this.indexesItemsStartEndJW = []; 
        this.indexesItemsStartEndDF = [];
        this.indexesItemsStartEndDS = []; 

        try {
            context.save();

            context.translate(0, ioStats.brushSectionHeight); 
            context.clearRect(0, 0, this.totalWidth, this.totalHeight - ioStats.brushSectionHeight);

            context.beginPath();
            context.rect(0, 0, this.totalWidth, this.totalHeight - ioStats.brushSectionHeight);
            context.clip();

            // 1. Draw tracks background 
            this.drawTracksBackground(context, xScale);

            // 2. Draw vertical dotted time lines & time labels in main section
            if (xScale.domain().length) {
                const ticks = this.getTicks(xScale);

                context.save();              
                context.rect(0, ioStats.axisHeight - 3, this.totalWidth, this.totalHeight - ioStats.brushSectionHeight);
                context.clip();
                this.drawXaxisTimeLines(context, ticks, this.yScale(this.data.Environments[0].Path) - 3, this.totalHeight);
                context.restore();

                this.drawXaxisTimeLabels(context, ticks, -20, 17);
            }          

            // 3. Draw all other data (track name + items on track)
            context.beginPath();
            context.rect(0, ioStats.axisHeight, this.totalWidth, this.totalHeight - ioStats.brushSectionHeight);
            context.clip(); 
            
            const extentFunc = gapFinder.extentGeneratorForScaleWithGaps(xScale);
            const indexesExpanded = this.isIndexesExpanded();

            for (let envIdx = 0; envIdx < this.data.Environments.length; envIdx++) {
                const env = this.data.Environments[envIdx];
                if (!this.filtered(env.Path)) {

                    // 3.1. Check if this is an index track 
                    let trackName = env.Path.substring(this.commonPathsPrefix.length);
                    let isIndexTrack = trackName.startsWith(ioStats.indexesString);

                    // 3.2 Draw track name
                    const yStart = this.yScale(env.Path);
                    this.drawTrackName(context, trackName, yStart);

                    const yStartPerTypeCache = new Map<Sparrow.MeterType, number>();
                    yStartPerTypeCache.set(ioStats.journalWriteString, yStart + ioStats.closedTrackHeight + ioStats.itemMargin);
                    yStartPerTypeCache.set(ioStats.dataFlushString, yStart + ioStats.closedTrackHeight + ioStats.itemMargin * 2 + ioStats.itemHeight);
                    yStartPerTypeCache.set(ioStats.dataSyncString, yStart + ioStats.closedTrackHeight + ioStats.itemMargin * 3 + ioStats.itemHeight * 2);

                    // 3.3 Draw item in main canvas area (but only if item is inside the visible/selected area from the brush section..)
                    for (let fileIdx = 0; fileIdx < env.Files.length; fileIdx++) {
                        const file = env.Files[fileIdx];
                       
                        for (let recentIdx = 0; recentIdx < file.Recent.length; recentIdx++) {
                            const recentItem = file.Recent[recentIdx] as IOMetricsRecentStatsWithCache;
                            const itemStartDateAsInt = recentItem.StartedAsDate.getTime();
                            const itemEndDateAsInt = recentItem.CompletedAsDate.getTime();

                            if (itemStartDateAsInt < visibleEndDateAsInt && itemEndDateAsInt > visibleStartDateAsInt) {
                                
                                // 3.4 Determine color for item
                                context.fillStyle = this.calcItemColor(recentItem, !isIndexTrack || indexesExpanded);

                                // 3.5 Determine yStart for item
                                const yStartItem = yStartPerTypeCache.get(recentItem.Type);

                                const x1 = xScale(recentItem.StartedAsDate);
                                let dx = extentFunc(recentItem.Duration);
                                dx = dx < ioStats.minItemWidth ? ioStats.minItemWidth : dx;

                                context.fillRect(x1, yStartItem, dx, ioStats.itemHeight);
                                
                                // 3.6 Draw the human size text on the item if there is enough space.. but don't draw on closed indexes track 
                                // Logic: Don't draw if: [closed && isIndexTrack] ==> [!(closed && isIndexTrack)] ==> [open || !isIndexTrack]
                                if (indexesExpanded || !isIndexTrack) {
                                    const humanSizeTextWidth = context.measureText(recentItem.HumanSize).width;
                                    if (dx > humanSizeTextWidth) {
                                        context.fillStyle = 'black';
                                        context.textAlign = "center";
                                        context.fillText(generalUtils.formatBytesToSize(recentItem.Size), x1 + dx / 2, yStartItem + ioStats.trackHeight / 2 + 4);
                                        // Note: there is a slight difference between Server side calculated 'HumanSize' &  the generalUtils calculation ....
                                    }

                                    // 3.7 Register track item for tooltip (but not for the 'closed' indexes track)
                                    this.hitTest.registerTrackItem(x1 - 2, yStartItem, dx + 2, ioStats.itemHeight, recentItem);
                                }
                                else {
                                    // 3.8 On the closed index track: 
                                    // Register toggle, so that indexes details will open
                                    this.hitTest.registerIndexToggle(x1 - 5, yStartItem, dx + 5, ioStats.itemHeight);

                                    // Register closed index track item for toolip
                                    this.hitTest.registerClosedTrackItem(x1 - 2, yStartItem, dx + 2, ioStats.itemHeight, recentItem);

                                    // Update closed index track StartEnd array (durations array), used in the tooltip on closed index track items
                                    switch (recentItem.Type) {
                                        case ioStats.journalWriteString: this.indexesItemsStartEndJW.push([itemStartDateAsInt, itemEndDateAsInt]); break;
                                        case ioStats.dataFlushString: this.indexesItemsStartEndDF.push([itemStartDateAsInt, itemEndDateAsInt]); break;
                                        case ioStats.dataSyncString: this.indexesItemsStartEndDS.push([itemStartDateAsInt, itemEndDateAsInt]); break;
                                    }
                                }
                            }
                        }
                    }
                };
            };

            // 4. Comact closed index track StartEnd durations array
            this.indexesItemsStartEndJW = this.compactDurationsInfo(this.indexesItemsStartEndJW);
            this.indexesItemsStartEndDF = this.compactDurationsInfo(this.indexesItemsStartEndDF);
            this.indexesItemsStartEndDS = this.compactDurationsInfo(this.indexesItemsStartEndDS);

            // 5. Draw gaps   
            this.drawGaps(context, xScale);
        }
        finally {
            context.restore();
        }
    }

    private compactDurationsInfo(durationsInfo: Array<[number, number]>): Array<[number, number]>{

        let joinedDurations: Array<[number, number]> = [];
        let joinedIndex = 0;

        // 1. Sort array (by start time, a[0] is start time, a[1] is end time)
        durationsInfo.sort((a, b) => a[0] === b[0] ? a[1] - b[1] : a[0] - b[0]);

        // 2. Join the overlapping array durations
        joinedDurations.push(durationsInfo[0]);

        for (let i = 1; i < durationsInfo.length; i++) {            
            const currentStart = durationsInfo[i][0];
            const currentEnd = durationsInfo[i][1];

            if (currentStart <= joinedDurations[joinedIndex][1]) {
                if (currentEnd > joinedDurations[joinedIndex][1]) {
                    joinedDurations[joinedIndex][1] = currentEnd;
                }
            }
            else {
                joinedIndex++;
                joinedDurations.push(durationsInfo[i]);
            }
        }

        return joinedDurations;
    }

    private filtered(envPath: string): boolean {             
        return _.includes(this.filteredIndexesTracksNames(), envPath.substring(this.commonPathsPrefix.length + 1 + ioStats.indexesString.length));
    }

    private drawTracksBackground(context: CanvasRenderingContext2D, xScale: d3.time.Scale<number, number>) {
        context.save();

        context.beginPath();
        context.rect(0, ioStats.axisHeight, this.totalWidth, this.totalHeight - ioStats.brushSectionHeight);
        context.clip();

        context.fillStyle = ioStats.colors.trackBackground;
        this.data.Environments.forEach(env => {  
             if (!this.filtered(env.Path)) {
                 context.fillRect(0, this.yScale(env.Path), this.totalWidth, ioStats.openedTrackHeight);
             }               
        });

        // The special case...draw the additional index heading when in expanded state
        if (this.isIndexesExpanded()) {
            context.fillRect(0, this.yScale(ioStats.indexesString), context.measureText(ioStats.indexesString).width + 30, ioStats.closedTrackHeight);
            this.drawTrackName(context, ioStats.indexesString, this.yScale(ioStats.indexesString));
        }

        context.restore();
    }

    private drawTrackName(context: CanvasRenderingContext2D, trackName: string, yStart: number) {
        const yTextShift = 14.5;
        const xTextShift = 0.5;
        let xTextStart = 5;
        let rectWidth;             
        let addedWidth = 8;
        let drawArrow = false;       
        let skipDrawing = false;

        const isIndexTrack = trackName.startsWith(ioStats.indexesString);

        // 1. Draw background color for track name - first check if track is an 'index' track
        if (isIndexTrack) {
            xTextStart = 15;
            addedWidth = 18;

            trackName = trackName.substring(ioStats.indexesString.length + 1);            

            // 1.1 The first indexes track has the track name of: 'Indexes' (both when opened or closed..)
            if ((trackName === "") || (!this.isIndexesExpanded())) {
                trackName = ioStats.indexesString;                
                addedWidth = 23;
                drawArrow = true;
                skipDrawing = this.allIndexesAreFiltered();
            }           
        }     

        if (!skipDrawing) {
            context.font = "12px Lato"; // Define font before using measureText()...
            rectWidth = context.measureText(trackName).width + addedWidth;
            context.fillStyle = ioStats.colors.trackNameBg;

            if (!_.includes(this.filteredIndexesTracksNames(), trackName)) {
                context.fillRect(2, yStart + ioStats.closedTrackPadding, rectWidth, ioStats.trackHeight);
            }

            // 2. Draw arrow only for indexes track
            if (drawArrow) {
                context.fillStyle = this.isIndexesExpanded() ? ioStats.colors.openedTrackArrow : ioStats.colors.closedTrackArrow;
                graphHelper.drawArrow(context, 5, yStart + 6, !this.isIndexesExpanded());
                this.hitTest.registerIndexToggle(2, yStart + ioStats.closedTrackPadding, rectWidth, ioStats.trackHeight);
            }

            // 3. Draw track name (if not filtered out..)                
            context.fillStyle = ioStats.colors.trackNameFg;
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
       
        context.beginPath();
        context.strokeStyle = ioStats.colors.gaps;
       
        for (let i = 1; i < range.length - 1; i += 2) {
            const gapX = Math.floor(range[i]) + 0.5;
         
            context.moveTo(gapX, ioStats.axisHeight);
            context.lineTo(gapX, this.totalHeight);          

            // Can't use xScale.invert here because there are Duplicate Values in xScale.range,
            // Using direct array access to xScale.domain instead
            const gapStartTime = xScale.domain()[i]; 
            const gapInfo = this.gapFinder.getGapInfoByTime(gapStartTime);

            if (gapInfo) {
                // Register gap for tooltip 
                this.hitTest.registerGapItem(gapX - 5, ioStats.axisHeight, 10, this.totalHeight,
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
        reader.onerror = function (error: any) {
            alert(error);
        };
        reader.readAsText(file);

        this.importFileName(fileInput.files[0].name);

        // Must clear the filePicker element value so that user will be able to import the -same- file after closing the imported view...
        let $input = $("#importFilePicker");
        $input.val(null);
    }

    private dataImported(result: string) {              
        this.cancelLiveView();

        try {
            const importedData: Raven.Server.Documents.Handlers.IOMetricsResponse = JSON.parse(result); 

            // Check if data is an IOStats json data..                                  
            if (!importedData.hasOwnProperty('Environments')) {
                messagePublisher.reportError("Invalid IO Stats file format", undefined, undefined);
            }
            else {
                if (this.hasAnyData()) {
                    this.resetGraphData();
                }
                this.data = importedData;
                this.fillCache();     
                this.prepareTimeData();
                this.initViewData();                
                this.setLegendScales();
                this.draw(true);
                this.isImport(true);
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

    closeImport() {       
        this.isImport(false);
        this.resetGraphData();
        this.enableLiveView();
    }   

    private resetGraphData() {
        this.data = null;
        this.searchText("");
        this.hasAnyData(false);   
        this.allIndexesAreFiltered(false);     
        this.setZoomAndBrush([0, this.totalWidth], brush => brush.clear());                           
    }

    private setZoomAndBrush(scale: [number, number], brushAction: (brush: d3.svg.Brush<any>) => void) {
        this.brushAndZoomCallbacksDisabled = true;

        this.xNumericScale.domain(scale);
        this.zoom.x(this.xNumericScale);

        brushAction(this.brush);
        this.brushContainer.call(this.brush);

        this.brushAndZoomCallbacksDisabled = false;
    }

    exportAsJson() {
        let exportFileName;

        if (this.isImport()) {
            exportFileName = this.importFileName().substring(0, this.importFileName().lastIndexOf('.'));
        }
        else {
            exportFileName = `IOStats-of-${this.activeDatabase().name}-${moment().format("YYYY-MM-DD-HH-mm")}`;
        }

        const keysToIgnore: Array<keyof IOMetricsRecentStatsWithCache> = ["StartedAsDate", "CompletedAsDate"];

        fileDownloader.downloadAsJson(this.data, exportFileName + ".json", exportFileName, (key, value) => {
            if (_.includes(keysToIgnore, key)) {
                return undefined;
            }
            return value;
        });
    }

    private findPrefix(strings: Array<string>) {
        if (!strings.length) {
            return "";
        }

        const sorted = strings.slice(0).sort();
        const string1 = sorted[0];
        const string2 = sorted[sorted.length - 1];
        let i = 0;
        const l = Math.min(string1.length, string2.length);

        while (i < l && string1[i] === string2[i]) {
            i++;
        }

        return string1.slice(0, i);
    }

    private calcItemColor(recentItem: Raven.Server.Documents.Handlers.IOMetricsRecentStats, calcColorBasedOnSize: boolean): string {
        let color: string;

        switch (recentItem.Type) {
            case ioStats.journalWriteString: {
                if (calcColorBasedOnSize) {                
                    color = this.legendJW().colorScale(recentItem.Size);
                }
                else {
                    color = ioStats.eventsColors.HighSizeColorJW;
                }
            } break;
            case ioStats.dataFlushString: {
                if (calcColorBasedOnSize) {   
                    color = this.legendDF().colorScale(recentItem.Size);
                }
                else {
                    color = ioStats.eventsColors.HighSizeColorDF;
                }
            } break;
            case ioStats.dataSyncString: {
                if (calcColorBasedOnSize) {
                    color = this.legendDS().colorScale(recentItem.Size);            
                }
                else {
                    color = ioStats.eventsColors.HighSizeColorDS;
                }
            } break;           
        }

        return color;
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

    private computedHumanSize(input: KnockoutObservable<number>): KnockoutComputed<string> {
        return ko.pureComputed(() => {
            return generalUtils.formatBytesToSize(input());
        });
    }

    /*
    * The following methods are called by hitTest class on mouse move    
    */

    private handleGapTooltip(element: timeGapInfo, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            const tooltipHtml = "Gap start time: " + (element).start.toLocaleTimeString() +
                "<br/>Gap duration: " + generalUtils.formatMillis((element).durationInMillis);
            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleClosedTrackTooltip(element: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) {
        const currentDatum = this.tooltip.datum();

        if (currentDatum !== element) {
            let typeString;
            let duration;

            switch (element.Type) {
                case ioStats.journalWriteString:
                    typeString = "Journal Write";
                    duration = this.getJoinedDuration(this.indexesItemsStartEndJW, element);
                    break;
                case ioStats.dataFlushString:
                    typeString = "Voron Data Flush";
                    duration = this.getJoinedDuration(this.indexesItemsStartEndDF, element);
                    break;
                case ioStats.dataSyncString:
                    typeString = "Voron Data Sync";
                    duration = this.getJoinedDuration(this.indexesItemsStartEndDS, element);
                    break;
            }
            let tooltipHtml = `*** ${typeString} ***<br/>`;       
            duration = (duration === 0) ? "0" : generalUtils.formatMillis(duration);                                      
            tooltipHtml += `Duration: ${duration}<br/>`;
            tooltipHtml += `Click for details`;           

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private getJoinedDuration(joinedDurationInfo: Array<[number, number]>, element: Raven.Server.Documents.Handlers.IOMetricsRecentStats): number {      
        const elementStart = new Date(element.Start).getTime();
        const elementEnd = new Date(elementStart + element.Duration).getTime();
       
        // Find containing part from the joined durations array
        let joinedDuration = joinedDurationInfo.find(duration => (duration[0] <= elementStart) && (elementEnd <= duration[1]));
        let duration = joinedDuration[1] - joinedDuration[0];       
        return duration;
    }

    private handleTrackTooltip(element: Raven.Server.Documents.Handlers.IOMetricsRecentStats, x: number, y: number) {     
        const currentDatum = this.tooltip.datum();

        // 1. Show item size position in the legend (in addition to showing the tooltip)
        this.itemHoveredJW(false);
        this.itemHoveredDF(false);
        this.itemHoveredDS(false);
               
        switch (element.Type) {           
            case ioStats.journalWriteString: {
                this.itemHoveredJW(true);
                this.itemSizePositionJW(this.legendJW().sizeScale.invert(element.Size).toString() + "px");
            } break;
            case ioStats.dataFlushString: {
                this.itemHoveredDF(true);
                this.itemSizePositionDF(this.legendDF().sizeScale.invert(element.Size).toString() + "px");
            } break;
            case ioStats.dataSyncString: {
                this.itemHoveredDS(true);               
                this.itemSizePositionDS(this.legendDS().sizeScale.invert(element.Size).toString() + "px");
            } break;
        }

        // 2. Show tooltip
        if (currentDatum !== element) {
            let typeString;
            switch (element.Type) {
                case ioStats.journalWriteString: typeString = "Journal Write"; break;               
                case ioStats.dataFlushString: typeString = "Voron Data Flush"; break;
                case ioStats.dataSyncString: typeString = "Voron Data Sync"; break;
            }
            let tooltipHtml = `*** ${typeString} ***<br/>`;
            let duration = (element.Duration === 0) ? "0" : generalUtils.formatMillis((element).Duration);
            tooltipHtml += `Duration: ${duration}<br/>`;
            tooltipHtml += `Size: ${generalUtils.formatBytesToSize(element.Size)}<br/>`; 
            tooltipHtml += `Size (bytes): ${element.Size.toLocaleString()}<br/>`;
            tooltipHtml += `Allocated Size: ${generalUtils.formatBytesToSize(element.FileSize)}<br/>`; 
            tooltipHtml += `Allocated Size (bytes): ${element.FileSize.toLocaleString()}<br/>`;

            this.handleTooltip(element, x, y, tooltipHtml);
        }
    }

    private handleTooltip(element: Raven.Server.Documents.Handlers.IOMetricsRecentStats | timeGapInfo, x: number, y: number, tooltipHtml: string) {
        if (element) {         
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
                .style("top", (y + 10) + "px");    
           
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
        this.tooltip
            .transition()
            .duration(250)
            .style("opacity", 0);

        this.tooltip.datum(null);

        // No need to show arrow position in legend any more..
        this.itemHoveredJW(false);
        this.itemHoveredDF(false);
        this.itemHoveredDS(false);
    }   
}

export = ioStats;