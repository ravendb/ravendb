import timeSeriesQueryResult = require("models/database/timeSeries/timeSeriesQueryResult");
import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require("d3");
import viewHelpers = require("common/helpers/view/viewHelpers");
import colorsManager = require("common/colorsManager");
import genUtils = require("common/generalUtils");

interface graphData {
    pointSeries: graphSeries<dataPoint>[];
    rangeSeries: graphSeries<dataRangePoint>[];
}

interface dataPoint {
    date: Date;
    value: number;
}

interface closestItem<T> {
    series: graphSeries<T>;
    closestIndex: number;
}

interface tooltipItem {
    dateDescription: string;
    name: string;
    value: string;
    series: graphSeries<any>;
    type: timeSeriesResultType;
}

interface dataRangePoint {
    from: Date;
    to: Date;
    value: number;
}

class graphSeries<TPoint> {
    onChange: () => void;
    uniqueId = _.uniqueId("ts-series-");
    visible = ko.observable<boolean>(true);
    name: string;
    points: TPoint[];
    
    constructor(name: string, points: TPoint[], onChange: () => void) {
        this.name = name;
        this.onChange = onChange;
        this.points = points;

        this.visible.subscribe(() => this.onChange());
    }
}

abstract class timeSeriesContainer<T> {
    documentId: string;
    name: string;
    value: timeSeriesQueryResultDto;
    abstract type: timeSeriesResultType;
    onChange: () => void;
    series = ko.observableArray<graphSeries<T>>();

    static readonly dateFormat = "DD/MM/YYYY HH:mm:ss";
    
    protected constructor(item: timeSeriesPlotItem, onChange: () => void) {
        this.documentId = item.documentId;
        this.name = item.name;
        this.value = item.value;
        this.onChange = onChange;
    }
    
    get sectionName() {
        return this.documentId + " - " + this.name;
    }

    getSeriesData(): graphSeries<T>[] {
        return this.series()
            .filter(x => x.visible());
    }
    
    abstract getClosestItems(pointInTime: Date): Array<closestItem<T>>;
    
    protected getSeriesValuesNames(valuesCount: number, dto: timeSeriesQueryResultDto) {
        const seriesValuesName = _.range(valuesCount).map((_, idx) => "Value #" + (idx + 1));

        if (dto && dto["@metadata"] && dto["@metadata"]["@timeseries-named-values"]) {
            const namedValues = dto["@metadata"]["@timeseries-named-values"];
            for (let i = 0; i < namedValues.length; i++) {
                seriesValuesName[i] = namedValues[i];
            }
        }
        
        return seriesValuesName;
    }
}

class groupedTimeSeriesContainer extends timeSeriesContainer<dataRangePoint> {
    type: timeSeriesResultType = "grouped";
    
    constructor(item: timeSeriesPlotItem, onChange: () => void) {
        super(item, onChange);

        const groupedValues = item.value.Results as Array<timeSeriesQueryGroupedItemResultDto>;
        const seriesPrefixNames = timeSeriesQueryResult.detectGroupKeys(groupedValues);
        const valuesCount = timeSeriesQueryResult.detectValuesCount(item.value);
        
        const seriesValuesName = this.getSeriesValuesNames(valuesCount, item.value);
        
        const dateFromPoints = groupedValues.map(x => new Date(x.From));
        const dateToPoints = groupedValues.map(x => new Date(x.To));
        
        const series = [] as Array<graphSeries<dataRangePoint>>;
        
        seriesPrefixNames.forEach(prefix => {
            seriesValuesName.forEach((valueName, valueIdx) => {
                const dataPoints: dataRangePoint[] = groupedValues.map((item, itemIdx) => ({
                    value: groupedValues[itemIdx][prefix][valueIdx],
                    from: dateFromPoints[itemIdx],
                    to: dateToPoints[itemIdx]
                }));
                
                series.push(new graphSeries<dataRangePoint>(prefix + " - " + valueName, dataPoints, this.onChange));
            });
        });
        
        this.series(series);
    }
    
    getClosestItems(pointInTime: Date): Array<closestItem<dataRangePoint>> {
        return this.series()
            .filter(x => x.visible())
            .map(x => {
                const approxIndex = _.sortedIndexBy<dataRangePoint>(x.points, { from: pointInTime } as dataRangePoint, p => p.from);
                
                if (approxIndex > 0) {
                    // check if point in time is between
                    const point = x.points[approxIndex - 1];

                    if (pointInTime < point.to) {
                        return {
                            series: x,
                            closestIndex: approxIndex - 1
                        } as closestItem<dataRangePoint>;    
                    }
                }
                return undefined;
            })
            .filter(x => !!x);
    }
}

class rawTimeSeriesContainer extends timeSeriesContainer<dataPoint> {
    type: timeSeriesResultType = "raw";

    constructor(item: timeSeriesPlotItem, onChange: () => void) {
        super(item, onChange);

        this.prepareSeries();
    }
    
    private prepareSeries() {
        const rawValues = this.value.Results as Array<timeSeriesRawItemResultDto>;
        const valuesCount = _.max(rawValues.map(x => x.Values.length));
        const seriesName = this.getSeriesValuesNames(valuesCount, this.value);
        
        const datePoints = rawValues.map(x => new Date(x.Timestamp));
        
        this.series(seriesName.map((name, seriesNameIdx) => {
            const dataPoints: dataPoint[] = rawValues.map((v, valuesIdx) => ({
                value: v.Values[seriesNameIdx],
                date: datePoints[valuesIdx]
            }));
            return new graphSeries<dataPoint>(name, dataPoints, this.onChange);
        }));
    }
 
    getClosestItems(pointInTime: Date): Array<closestItem<dataPoint>> {
        return this.series()
            .filter(x => x.visible())
            .map(x => {
                const approxIndex = _.clamp(_.sortedIndexBy<dataPoint>(x.points, { date: pointInTime } as dataPoint, p => p.date), 0, x.points.length - 1);

                let effectiveIndex = approxIndex;
                if (effectiveIndex > 0) {
                    // check which item is closer: approxIndex - 1, or approxIndex
                    
                    const leftDistance = pointInTime.getTime() - x.points[approxIndex - 1].date.getTime();
                    const rightDistance = x.points[approxIndex].date.getTime() - pointInTime.getTime();

                    if (leftDistance < rightDistance) {
                        // use previous value
                        effectiveIndex--;
                    }
                }
                
                return {
                    series: x,
                    closestIndex: effectiveIndex
                } as closestItem<dataPoint>;
            });
    }
}

class timeSeriesPlotDetails extends viewModelBase {
    
    private readonly margin = {
        top: 40,
        left: 70,
        right: 40,
        bottom: 40,
        betweenGraphs: 50
    };
    
    colors = {
        "color-1": undefined as string,
        "color-2": undefined as string,
        "color-3": undefined as string,
        "color-4": undefined as string,
        "color-5": undefined as string,
        "color-6": undefined as string,
        "color-7": undefined as string,
        "color-8": undefined as string,
        "color-9": undefined as string,
        "color-10": undefined as string
    } as Record<string, string>;
    
    private readonly heightBrush = 80;
    
    pointTimeSeries: timeSeriesContainer<dataPoint>[] = [];
    rangeTimeSeries: timeSeriesContainer<dataRangePoint>[] = [];

    private containerWidth: number;
    private containerHeight: number;
    
    private cachedData: graphData;
    
    private width: number;
    private heightGraph: number;
    
    private x: d3.time.Scale<number, number>;
    private y: d3.scale.Linear<number, number>;
    private emptyGraph = ko.observable<boolean>(false);
    
    private xBrush: d3.time.Scale<number, number>;
    private yBrush: d3.scale.Linear<number, number>;
    
    private xAxis: d3.svg.Axis;
    private xAxisBrush: d3.svg.Axis;
    private yAxis: d3.svg.Axis;
    
    private brush: d3.svg.Brush<void>;

    private focusCanvas: d3.Selection<any>;
    private contextCanvas: d3.Selection<any>;

    private pointer: d3.Selection<void>;
    private tooltip: d3.Selection<void>;
    private acceptMoveEvents: boolean = false;
    
    private svg: d3.Selection<void>;
    private hoverSvg: d3.Selection<void>;
    private focus: d3.Selection<void>;
    private hoverFocus: d3.Selection<void>;
    private context: d3.Selection<void>;
    
    private zoom: d3.behavior.Zoom<void>;
    private rect: d3.Selection<any>;
    private readonly colorClassScale: d3.scale.Ordinal<string, keyof this["colors"] & string>;

    connectPoints = ko.observable<boolean>(true);
    
    private throttledTooltipUpdate = _.throttle((svgLocation: [number, number], globalLocation: [number, number]) => this.updateTooltip(svgLocation, globalLocation), 100);
    
    documentIds: Array<string> = [];
    
    constructor(timeSeries: Array<timeSeriesPlotItem>) {
        super();
        
        const onChange = () => this.draw(true, false);
        
        this.connectPoints.subscribe(() => onChange());
        
        timeSeries.forEach(item => {
            const value = item.value;
            const type = timeSeriesQueryResult.detectResultType(value);

            switch (type) {
                case "grouped":
                    this.rangeTimeSeries.push(new groupedTimeSeriesContainer(item, onChange));
                    break;
                case "raw":
                    this.pointTimeSeries.push(new rawTimeSeriesContainer(item, onChange));
                    break;
            }
            
            this.documentIds.push(item.documentId);
        });
        
        _.bindAll(this, "getColor", "getColorClass");

        this.colorClassScale = d3.scale.ordinal<string>()
            .range(_.range(1, 11).map(x => "color-" + x));
    }
    
    getColorClass(series: graphSeries<any>) {
        return this.colorClassScale(series.uniqueId);
    }
    
    getIcon(type: timeSeriesResultType) {
        switch (type) {
            case "raw":
                return "icon-graph";
            case "grouped":
                return "icon-graph-range";
        }
    }
    
    getColor(series: graphSeries<any>) {
        return this.colors[this.getColorClass(series)];
    }
    
    get allTimeSeries() {
        return [...this.rangeTimeSeries, ...this.pointTimeSeries];
    }
    
    compositionComplete() {
        super.compositionComplete();

        this.tooltip = d3.select(".ts-tooltip");

        colorsManager.setup(".time-series-details", this.colors);
        
        this.initGraph();
        this.draw(true, true);
    }
    
    private initGraph() {
        [this.containerWidth, this.containerHeight] = viewHelpers.getPageHostDimenensions();
        
        this.width = this.containerWidth 
            - this.margin.left - this.margin.right;
        
        this.heightGraph = this.containerHeight 
            - this.heightBrush 
            - this.margin.betweenGraphs - this.margin.top - this.margin.bottom;
        
        this.x = d3.time.scale<number, number>()
            .range([0, this.width]);
        this.xBrush = d3.time.scale<number>()
            .range([0, this.width]);
        this.y = d3.scale.linear()
            .range([this.heightGraph, 0]);
        this.yBrush = d3.scale.linear()
            .range([this.heightBrush, 0]);
        
        this.xAxis = d3.svg.axis()
            .scale(this.x)
            .orient("bottom");
        
        this.xAxisBrush = d3.svg.axis()
            .scale(this.xBrush)
            .orient("bottom");
        
        this.yAxis = d3.svg.axis()
            .scale(this.y)
            .ticks(10, "s")
            .orient("left");
        
        this.brush = d3.svg.brush<void>()
            .x(this.xBrush as any)
            .on("brush", () => this.onBrushed());
        
        const container = d3.select(".time-series-details .dynamic-container");

        this.contextCanvas = container
            .append("canvas")
            .attr("class", "context-canvas")
            .attr("width", this.width)
            .attr("height", this.heightBrush)
            .style({
                "top": (this.margin.top + this.heightGraph + this.margin.betweenGraphs) + "px",
                "left": this.margin.left + "px"
            });
        
        this.svg = container
            .append("svg:svg")
            .attr("width", this.containerWidth)
            .attr("height", this.containerHeight);

        this.focusCanvas = container
            .append("canvas")
            .attr("class", "focus-canvas")
            .attr("width", this.width)
            .attr("height", this.heightGraph)
            .style({
                "top": this.margin.top + "px",
                "left": this.margin.left + "px"
            });
        
        this.svg
            .append("defs")
            .append("clipPath")
            .attr("id", "clip")
            .append("rect")
            .attr("width", this.width)
            .attr("height", this.heightGraph);
        
        this.focus = this.svg.append("g")
            .attr("class", "focus")
            .attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")");
        
        this.context = this.svg.append("g")
            .attr("class", "context")
            .attr("transform", "translate(" + this.margin.left + "," + (this.margin.top + this.heightGraph + this.margin.betweenGraphs) + ")");
        
        this.focus.append("g")
            .attr("class", "grid-x");
        
        this.focus.append("g")
            .attr("class", "grid-y");
        
        this.zoom = d3.behavior.zoom<void>()
            .on("zoom", () => this.draw(false, false));

        this.focus.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + this.heightGraph + ")");

        this.focus.append("g")
            .attr("class", "y axis");
        
        this.context.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + this.heightBrush + ")");

        // Add rect cover the zoomed graph and attach zoom event.
        this.rect = this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.width)
            .attr("height", this.heightGraph)
            .attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")")
            .call(this.zoom);

        this.context.append("g")
            .attr("class", "x brush")
            .call(this.brush)
            .selectAll("rect")
            .attr("y", 1)
            .attr("height", this.heightBrush - 1);

        this.hoverSvg = container
            .append("svg:svg")
            .attr("class", "hover-area")
            .attr("width", this.containerWidth)
            .attr("height", this.containerHeight);

        const pointer = this.hoverSvg
            .append("g")
            .attr("class", "pointer");
        
        this.hoverFocus = this.hoverSvg.append("g")
            .attr("class", "hover-focus")
            .attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")");
        
        this.hoverFocus.append("g")
            .attr("class", "ranges");
        
        this.hoverFocus.append("g")
            .attr("class", "points");
        
        this.pointer = pointer.append("line")
            .attr("class", "pointer-line")
            .attr("x1", 0)
            .attr("x2", 0)
            .attr("y1", this.margin.top)
            .attr("y2", this.margin.top + this.heightGraph)
            .style("stroke-opacity", 0);

        this.setupMouseEvents();
    }
    
    private setupMouseEvents() {
        this.svg
            .select(".pane")
            .on("mousedown.tip", () => this.hideTooltip())
            .on("mouseup.tip", () => this.showTooltip())
            .on("mouseenter.tip", () => this.showTooltip())
            .on("mouseleave.tip", () => this.hideTooltip())
            .on("mousemove.tip", () => {
                if (this.acceptMoveEvents) {
                    const node = this.svg.node();
                    const mouseLocation = d3.mouse(node);
                    this.pointer
                        .attr("x1", mouseLocation[0] + 0.5)
                        .attr("x2", mouseLocation[0] + 0.5);

                    const svgLocation = d3.mouse(this.svg.node());
                    const globalLocation = d3.mouse(d3.select(".time-series-details").node());
                    this.throttledTooltipUpdate(svgLocation, globalLocation);
                }
            });
    }
    
    private showTooltip() {
        this.acceptMoveEvents = true;
        
        this.pointer
            .transition()
            .duration(200)
            .style("stroke-opacity", 1);
        
        this.tooltip
            .style("display", undefined)
            .style("opacity", 1);

        const svgLocation = d3.mouse(this.svg.node());
        this.updateHighlightInfo(svgLocation);
    }
    
    private hideTooltip() {
        this.pointer
            .transition()
            .duration(100)
            .style("stroke-opacity", 0);

        this.acceptMoveEvents = false;
        
        this.updateHighlightInfo(null);
        this.tooltip.html("");
        
        this.tooltip.transition("opacity")
            .duration(250)
            .style("opacity", 0)
            .each("end", () => this.tooltip.style('display', 'none'));
    }
    
    private updateTooltip(svgLocation: [number, number], globalLocation: [number, number]) {
        const [x, y] = globalLocation;
        
        this.tooltip
            .transition("move");
        
        this.tooltip
            .style("display", undefined)
            .transition("move")
            .delay(30)
            .duration(200)
            .style("left", (x + 10) + "px")
            .style("top", (y + 10) + "px");
        
        this.updateHighlightInfo(svgLocation);
    }
    
    private updateHighlightInfo(location: [number, number]) {
        if (location) {
            const cursorDate = this.x.invert(location[0] - this.margin.left);

            const rangeItems = _.flatten(this.rangeTimeSeries.map(x => x.getClosestItems(cursorDate)));
            const pointItems = _.flatten(this.pointTimeSeries.map(x => x.getClosestItems(cursorDate)));

            const tooltipHtml = this.tooltipContents(rangeItems, pointItems);
            this.tooltip.html(tooltipHtml);
            
            const onlyDefinedRangeItems = rangeItems
                .filter(x => typeof x.series.points[x.closestIndex].value !== "undefined");
            const onlyDefinedPointItems = pointItems
                .filter(x => typeof x.series.points[x.closestIndex].value !== "undefined");

            const points = this.hoverFocus
                .select(".points")
                .selectAll(".point")
                .data(onlyDefinedPointItems, x => x.closestIndex + "@" + x.series.uniqueId);

            points
                .attr("cx", d => this.x(d.series.points[d.closestIndex].date))
                .attr("cy", d => this.y(d.series.points[d.closestIndex].value));

            points.exit()
                .filter(":not(.processed)")
                .classed("processed", true)
                .transition("circle")
                .attr("r", 0)
                .remove();

            points
                .enter()
                .append("circle")
                .attr("class", x => "point " + this.getColorClass(x.series))
                .attr("r", 0)
                .attr("cx", d => this.x(d.series.points[d.closestIndex].date))
                .attr("cy", d => this.y(d.series.points[d.closestIndex].value))
                .transition("circle")
                .attr("r", 4);
            
            const ranges = this.hoverFocus
                .select(".ranges")
                .selectAll(".range")
                .data(onlyDefinedRangeItems, x => x.closestIndex + "@" + x.series.uniqueId);
            
            const computeX = (d: closestItem<dataRangePoint>, accessor: (point: dataRangePoint) => Date) => {
                const point = d.series.points[d.closestIndex];
                const x = this.x(accessor(point));
                return _.clamp(x, 0, this.width);
            };
            
            ranges
                .attr("x1", d => computeX(d, p => p.from))
                .attr("x2", d => computeX(d, p => p.to));
            
            ranges.exit()
                .transition()
                .style("opacity", 0)
                .remove();
            
            ranges.enter()
                .append("line")
                .attr("class", x => "range " + this.getColorClass(x.series))
                .attr("x1", d => computeX(d, p => p.from))
                .attr("x2", d => computeX(d, p => p.to))
                .attr("y1", d => this.y(d.series.points[d.closestIndex].value))
                .attr("y2", d => this.y(d.series.points[d.closestIndex].value))
                .style("opacity", 0)
                .transition()
                .style("opacity", 1);
        } else {
            this.hoverFocus
                .select(".points")
                .selectAll(".point")
                .remove();
            
            this.hoverFocus
                .select(".ranges")
                .selectAll(".range")
                .remove();
        }
    }
    
    private tooltipContents(rangeItems: closestItem<dataRangePoint>[], pointItems: closestItem<dataPoint>[]) {
        const formatDate = (d: Date) => moment.utc(d).local().format(timeSeriesContainer.dateFormat);
        
        const mappedRange = rangeItems.map(x => {
            const closestItem = x.series.points[x.closestIndex];
            const date = formatDate(closestItem.from) + " - " + formatDate(closestItem.to);
            
            return {
                name: x.series.name,
                series: x.series,
                dateDescription: date,
                type: "grouped",
                value: typeof closestItem.value !== "undefined" ? closestItem.value.toLocaleString() : "N/A"
            } as tooltipItem;
        });
        
        const mappedPoints = pointItems.map(x => {
            const closestItem = x.series.points[x.closestIndex];
            const date = formatDate(closestItem.date);

            return {
                series: x.series,
                name: x.series.name,
                dateDescription: date,
                type: "raw",
                value: typeof closestItem.value !== "undefined" ? closestItem.value.toLocaleString() : "N/A"
            } as tooltipItem;
        });
        
        return genUtils.inMemoryRender("time-series-graph-tooltip", {
            items: [...mappedRange, ...mappedPoints],
            getColorClass: this.getColorClass
        });
    }
    
    private onBrushed() {
        this.x
            .domain(this.brush.empty() ? this.xBrush.domain() : this.brush.extent() as any);
        
        this.zoom.x(this.x as any);
        
        this.draw(false, false);
    }
    
    private draw(dataUpdated: boolean, resetXScale: boolean) {
        if (dataUpdated) {
            this.cachedData = this.getDataToPlot();
        }

        if (d3.event) {
            // if we can access surrounding event, then redraw highlights  
            const svgLocation = d3.mouse(this.svg.node());
            this.updateHighlightInfo(svgLocation);
        }
        
        const data = this.cachedData;

        const focusCanvas = this.focusCanvas.node() as HTMLCanvasElement;
        const focusContext = focusCanvas.getContext("2d");

        const contextCanvas = this.contextCanvas.node() as HTMLCanvasElement;
        const contextContext = contextCanvas.getContext("2d");
        
        if (dataUpdated) {
            if (data.pointSeries.length || data.rangeSeries.length) {
                const extents = timeSeriesPlotDetails.computeExtents(data);
                const paddedExtents = timeSeriesPlotDetails.paddingExtents(extents, 0.02);
                const { minX, maxX, minY, maxY } = paddedExtents;

                if (resetXScale || this.emptyGraph()) {
                    this.x.domain([minX, maxX]);
                }
                this.y.domain([minY, maxY]);
                this.emptyGraph(false);
            } else {
                const now = new Date();
                this.x.domain([now, now]);
                this.y.domain([0, 0]);
                this.emptyGraph(true);
            }

            if (resetXScale) {
                this.xBrush.domain(this.x.domain());
            }
            this.yBrush.domain(this.y.domain());
            this.zoom.x(this.x as any);

            this.context.select(".x.axis")
                .call(this.xAxisBrush);
        }

        this.focus.select(".x.axis")
            .call(this.xAxis);

        this.focus.select(".y.axis")
            .call(this.yAxis);
        
        if (dataUpdated) {
            contextContext.clearRect(0, 0, this.width, this.heightGraph);

            try {
                contextContext.save();

                // range series
                for (let i = 0; i < data.rangeSeries.length; i++) {
                    const series = data.rangeSeries[i];
                    contextContext.beginPath();
                    contextContext.strokeStyle = this.getColor(series);
                    contextContext.lineWidth = 1;
                    for (let p = 0; p < series.points.length; p++) {
                        const point = series.points[p];
                        const yValue = this.yBrush(point.value);
                        if (this.connectPoints()) {
                            contextContext.lineTo(this.xBrush(point.from), yValue);
                            contextContext.lineTo(this.xBrush(point.to), yValue);
                        } else {
                            const x1 = this.xBrush(point.from);
                            const x2 = this.xBrush(point.to);
                            const xDelta = Math.max(x2 - x1, 1);
                            contextContext.moveTo(x1, yValue);
                            contextContext.lineTo(x1 + xDelta, yValue);
                        }
                    }
                    contextContext.stroke();
                }

                const pixelTimeDelta = this.xBrush.invert(1).getTime() - this.xBrush.invert(0).getTime();
                
                // point series
                for (let i = 0; i < data.pointSeries.length; i++) {
                    const points = data.pointSeries[i];

                    if (points.points.length) {
                        contextContext.beginPath();
                        contextContext.strokeStyle = this.colors[this.colorClassScale(points.uniqueId)];
                        contextContext.lineWidth = 1;
                        contextContext.fillStyle = this.colors[this.colorClassScale(points.uniqueId)];
                        const renderer = new quantizedLineRenderer(contextContext, pixelTimeDelta, this.xBrush, this.yBrush, this.connectPoints());
                        renderer.draw(points.points[0].date, points.points[0].value);

                        for (let p = 0; p < points.points.length; p++) {
                            const point = points.points[p];
                            renderer.draw(point.date, point.value);
                        }

                        renderer.flush();
                        contextContext.stroke();
                    }
                }
            } finally {
                contextContext.restore();
            }
        }

        focusContext.clearRect(0, 0, this.width, this.heightGraph);

        try {
            focusContext.save();
            const visibleRange = [this.x.invert(0), this.x.invert(this.width)];
            
            // range series
            for (let i = 0; i < data.rangeSeries.length; i++) {
                const series = data.rangeSeries[i];
                focusContext.beginPath();
                focusContext.strokeStyle = this.getColor(series);
                focusContext.lineWidth = 1;
                for (let p = 0; p < series.points.length; p++) {
                    const point = series.points[p];
                    const yValue = this.y(point.value);
                    if (this.connectPoints()) {
                        focusContext.lineTo(this.x(point.from), yValue);
                        focusContext.lineTo(this.x(point.to), yValue);
                    } else {
                        const x1 = this.x(point.from);
                        const x2 = this.x(point.to);
                        const xDelta = Math.max(x2 - x1, 1);
                        focusContext.moveTo(x1, yValue);
                        focusContext.lineTo(x1 + xDelta, yValue);
                    }
                }
                focusContext.stroke();
            }

            const pixelTimeDelta = this.x.invert(1).getTime() - this.x.invert(0).getTime();
            
            // point series
            for (let i = 0; i < data.pointSeries.length; i++) {
                const points = data.pointSeries[i];
                
                const startIdx = Math.max(_.sortedIndexBy<dataPoint>(points.points, { date: visibleRange[0] } as dataPoint, x => x.date) - 1, 0); 
                const endIdx = Math.min(points.points.length, _.sortedIndexBy<dataPoint>(points.points, { date: visibleRange[1] } as dataPoint, x => x.date) + 1); 
                
                if (points.points.length) {
                    focusContext.beginPath();
                    focusContext.strokeStyle = this.colors[this.colorClassScale(points.uniqueId)];
                    focusContext.lineWidth = 1;
                    const renderer = new quantizedLineRenderer(focusContext, pixelTimeDelta, this.x, this.y, this.connectPoints());
                    renderer.draw(points.points[startIdx].date, points.points[startIdx].value);

                    for (let p = startIdx; p < endIdx; p++) {
                        const point = points.points[p];
                        renderer.draw(point.date, point.value);
                    }
                    
                    renderer.flush();
                    focusContext.stroke();
                }
            }
            
        } finally {
            focusContext.restore();
        }
        
        this.brush.extent(this.x.domain() as any);
        
        this.svg.select(".brush")
            .call(this.brush);
        
        this.drawGrid();
    }
    
    private drawGrid() {
        const xTicks = this.x.ticks();
        
        const xGrid = this.svg.select(".grid-x")
            .selectAll(".grid")
            .data<Date>(xTicks, x => x.getTime().toString());
     
        xGrid.enter()
            .append("line")
            .attr("class", "grid");

        xGrid
            .exit()
            .remove();
        
        xGrid
            .attr("x1", d => this.x(d))
            .attr("x2", d => this.x(d))
            .attr("y1", 0)
            .attr("y2", this.heightGraph);
        
        const yTicks = this.y.ticks();
        
        const yGrid = this.svg.select(".grid-y")
            .selectAll(".grid")
            .data<number>(yTicks, x => x.toString());
        
        yGrid.enter()
            .append("line")
            .attr("class", "grid");
        
        yGrid
            .exit()
            .remove();
        
        yGrid
            .attr("x1", 0)
            .attr("x2", this.width)
            .attr("y1", d => this.y(d))
            .attr("y2", d => this.y(d));
    }
    
    private static computePointExtents(series: graphSeries<dataPoint>[]) {
        const nonEmptySeries = series.filter(x => x.points.length);
        
        const minX = d3.min(nonEmptySeries.map(x => x.points[0].date));
        const maxX = d3.max(nonEmptySeries.map(x => x.points[x.points.length - 1].date));

        const yExtendsList = nonEmptySeries.map(x => d3.extent(x.points.map(y => y.value)));
        const minY = d3.min(yExtendsList.map(x => x[0]));
        const maxY = d3.max(yExtendsList.map(x => x[1]));
        
        return {
            minX, maxX, 
            minY, maxY
        };
    }

    private static computeRangeExtents(series: graphSeries<dataRangePoint>[]) {
        const nonEmptySeries = series.filter(x => x.points.length);

        const minX = d3.min(nonEmptySeries.map(x => x.points[0].from));
        const maxX = d3.max(nonEmptySeries.map(x => x.points[x.points.length - 1].to));

        const yExtendsList = nonEmptySeries.map(x => d3.extent(x.points.map(y => y.value)));
        const minY = d3.min(yExtendsList.map(x => x[0]));
        const maxY = d3.max(yExtendsList.map(x => x[1]));

        return {
            minX, maxX,
            minY, maxY
        };
    }
    
    private static computeExtents(data: graphData) {
        const pointsExtents = timeSeriesPlotDetails.computePointExtents(data.pointSeries);
        const rangeExtents = timeSeriesPlotDetails.computeRangeExtents(data.rangeSeries);
        
        return {
            minX: d3.min([pointsExtents.minX, rangeExtents.minX]),
            maxX: d3.max([pointsExtents.maxX, rangeExtents.maxX]),
            minY: d3.min([pointsExtents.minY, rangeExtents.minY, 0]),
            maxY: d3.max([pointsExtents.maxY, rangeExtents.maxY, 0])
        }
    }
    
    private static paddingExtents(extents: { minX: Date, maxX: Date, minY: number, maxY: number }, percentagePadding: number) {
        // please notice this function doesn't padding zero
        
        const deltaX = extents.maxX.getTime() - extents.minX.getTime();
        const deltaY = extents.maxY - extents.minY;
        
        const xShift = deltaX * percentagePadding / 2;
        const yShift = deltaY * percentagePadding / 2;
        
        return {
            minX: new Date(extents.minX.getTime() - xShift),
            maxX: new Date(extents.maxX.getTime() + xShift),
            minY: extents.minY === 0 ? 0 :extents.minY - yShift,
            maxY: extents.maxY === 0 ? 0 : extents.maxY + yShift
        }
    }
    
    getDataToPlot(): graphData {
        const result: graphData = {
            pointSeries: [],
            rangeSeries: []
        };
        
        this.pointTimeSeries.forEach(item => {
            result.pointSeries.push(...item.getSeriesData());
        });
        
        this.rangeTimeSeries.forEach(item => {
            result.rangeSeries.push(...item.getSeriesData());
        });
        
        return result;
    }
}


class quantizedLineRenderer {
    
    private first = true;
    
    private queue: {
        values: number[],
        date: Date
    };
    
    constructor(private context: CanvasRenderingContext2D, 
                private quantizationOffset: number, 
                private xScale: d3.time.Scale<number, number>, 
                private yScale: d3.scale.Linear<number, number>,
                private connectPoints: boolean) {
    }
    
    private flushQueue() {
        if (!this.queue) {
            return;
        }
        
        if (this.queue.values.length > 1) {
            // compute extend to avoid drawing average line
            const [localMin, localMax] = d3.extent(this.queue.values);
            const xValue = this.xScale(this.queue.date);
            const yMin = this.yScale(localMin);
            const yMax = this.yScale(localMax);
            
            if (this.connectPoints) {
                this.context.lineTo(xValue, yMin);
                this.context.lineTo(xValue, yMax);    
            } else {
                const yToDraw = new Set<number>();
                for (let i = 0; i < this.queue.values.length; i++) {
                    yToDraw.add(Math.floor(this.yScale(this.queue.values[i])));
                }
                
                const drawArray = Array.from(yToDraw);
                for (let i = 0; i < drawArray.length; i++) {
                    this.context.strokeRect(xValue, drawArray[i], 1, 1);
                }
            }
        } else {
            if (this.connectPoints) {
                this.context.lineTo(this.xScale(this.queue.date), this.yScale(this.queue.values[0]));    
            } else {
                this.context.strokeRect(this.xScale(this.queue.date), this.yScale(this.queue.values[0]), 1, 1);
            }
        }
        
        this.queue = null;
    }
    
    draw(date: Date, value: number) {
        if (this.first) {
            this.first = false;
            this.context.moveTo(this.xScale(date), this.yScale(value));
        } else {
            if (this.queue) {
                const shouldFlush = this.queue.date.getTime() + this.quantizationOffset < date.getTime();
                if (shouldFlush) {
                    this.flushQueue();
                    this.queue = {
                        values: [value],
                        date: date
                    }
                } else {
                    this.queue.values.push(value);
                }
            } else {
                // queue is empty - append item
                this.queue = {
                    values: [value],
                    date: date
                };
            }
        }
    }
    
    flush() {
        this.flushQueue();
    }
}

export = timeSeriesPlotDetails;
