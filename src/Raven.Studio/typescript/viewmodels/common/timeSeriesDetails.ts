import document = require("models/database/documents/document");
import timeSeriesQueryResult = require("models/database/timeSeries/timeSeriesQueryResult");
import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require("d3");
import viewHelpers = require("common/helpers/view/viewHelpers");

type timeSeriesItem = {
    document: document;
    path: string;
}

interface graphData {
    pointSeries: graphSeries<dataPoint>[];
    rangeSeries: graphSeries<dataRangePoint>[];
}

interface dataPoint {
    date: Date;
    value: number;
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
    sourceDocument: document;
    path: string;
    value: timeSeriesQueryResultDto;
    abstract type: timeSeriesResultType;
    onChange: () => void;
    series = ko.observableArray<graphSeries<T>>();
    
    protected constructor(item: timeSeriesItem, onChange: () => void) {
        this.sourceDocument = item.document;
        this.path = item.path;
        this.value = (item.document as any)[item.path] as timeSeriesQueryResultDto;
        this.onChange = onChange;
    }
    
    get sectionName() {
        return this.sourceDocument.getId() + " - " + this.path;
    }

    getSeriesData(): graphSeries<T>[] {
        return this.series()
            .filter(x => x.visible());
    }
}

class groupedTimeSeriesContainer extends timeSeriesContainer<dataRangePoint> {
    type: timeSeriesResultType = "grouped";
    
    constructor(item: timeSeriesItem, onChange: () => void) {
        super(item, onChange);

        const groupedValues = this.value.Results as Array<timeSeriesQueryGroupedItemResultDto>;
        const allKeys = Object.keys(groupedValues[0]);
        const seriesPrefixNames = _.without(allKeys, "From", "To", "Count");
        
        const valuesCount = groupedValues[0][seriesPrefixNames[0]].length; //TODO: scan through all values!
        const seriesValuesName = _.range(valuesCount).map((_, idx) => "Value #" + (idx + 1));
        
        const dateFromPoints = groupedValues.map(x => moment.utc(x.From).toDate());
        const dateToPoints = groupedValues.map(x => moment.utc(x.To).toDate());
        
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
}

class rawTimeSeriesContainer extends timeSeriesContainer<dataPoint> {
    type: timeSeriesResultType = "raw";

    constructor(item: timeSeriesItem, onChange: () => void) {
        super(item, onChange);

        this.prepareSeries();
    }

    private prepareSeries() { //TODO: check if variable values length! - use max!
        const rawValues = this.value.Results as Array<timeSeriesRawItemResultDto>;
        const valuesCount = rawValues[0].Values.length;
        const seriesName = _.range(valuesCount).map((_, idx) => "Value #" + (idx + 1));
        
        const datePoints = rawValues.map(x => moment.utc(x.Timestamp).toDate());
        
        this.series(seriesName.map((name, seriesNameIdx) => {
            const dataPoints: dataPoint[] = rawValues.map((v, valuesIdx) => ({
                value: v.Values[seriesNameIdx],
                date: datePoints[valuesIdx]
            }));
            return new graphSeries<dataPoint>(name, dataPoints, this.onChange);
        }));
    }
}

type viewMode = "plot" | "table";

class timeSeriesDetails extends viewModelBase {
    
    private readonly margin = {
        top: 40,
        left: 40,
        right: 40,
        bottom: 40,
        betweenGraphs: 50
    };
    
    private readonly heightBrush = 80;
    
    private mode = ko.observable<viewMode>();
    pointTimeSeries: timeSeriesContainer<dataPoint>[] = [];
    rangeTimeSeries: timeSeriesContainer<dataRangePoint>[] = [];

    private containerWidth: number;
    private containerHeight: number;
    
    private width: number;
    private heightGraph: number;
    
    private x: d3.time.Scale<number, number>;
    private y: d3.scale.Linear<number, number>;
    
    private xBrush: d3.time.Scale<number, number>;
    private yBrush: d3.scale.Linear<number, number>;
    
    private xAxis: d3.svg.Axis;
    private xAxisBrush: d3.svg.Axis;
    private yAxis: d3.svg.Axis;
    
    private brush: d3.svg.Brush<void>;
    
    private svg: d3.Selection<void>;
    private focus: d3.Selection<void>;
    private context: d3.Selection<void>;
    private line: d3.svg.Line<dataPoint>;
    private lineBrush: d3.svg.Line<dataPoint>;
    
    private zoom: d3.behavior.Zoom<void>;
    private rect: d3.Selection<any>;
    private colorClassPointScale: d3.scale.Ordinal<string, string>;
    private colorClassRangeScale: d3.scale.Ordinal<string, string>;
    
    constructor(timeSeries: Array<timeSeriesItem>, initialMode: viewMode = "plot") { //TODO: support modes!
        super();
        
        const onChange = () => this.draw(true, false);
        
        timeSeries.forEach(item => {
            const value = (item.document as any)[item.path] as timeSeriesQueryResultDto;
            const type = timeSeriesQueryResult.detectResultType(value);

            switch (type) {
                case "grouped":
                    this.rangeTimeSeries.push(new groupedTimeSeriesContainer(item, onChange));
                    break;
                case "raw":
                    this.pointTimeSeries.push(new rawTimeSeriesContainer(item, onChange));
                    break;
            }
        });
        
        this.mode(initialMode);

        this.colorClassPointScale = d3.scale.ordinal<string>()
            .range(_.range(1, 10).map(x => "color-" + x));

        this.colorClassRangeScale = d3.scale.ordinal<string>()
            .range(_.range(1, 10).map(x => "color-" + x));
    }
    
    get allTimeSeries() {
        return [...this.rangeTimeSeries, ...this.pointTimeSeries];
    }
    
    compositionComplete() {
        super.compositionComplete();
        
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
            .orient("left");
        
        this.brush = d3.svg.brush<void>()
            .x(this.xBrush as any)
            .on("brush", () => this.onBrushed());
        
        this.line = d3.svg.line<dataPoint>()
            .x(x => this.x(x.date))
            .y(x => this.y(x.value));
        
        this.lineBrush = d3.svg.line<dataPoint>()
            .x(x => this.xBrush(x.date))
            .y(x => this.yBrush(x.value));
        
        this.svg = d3.select(".time-series-details .dynamic-container")
            .append("svg:svg")
            .attr("width", this.containerWidth)
            .attr("height", this.containerHeight);
        
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

        // Add rect cover the zoomed graph and attach zoom event.
        this.rect = this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.width)
            .attr("height", this.heightGraph)
            .attr("transform", "translate(" + this.margin.left + "," + this.margin.top + ")")
            .call(this.zoom);

        this.focus.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + this.heightGraph + ")");
        
        this.focus.append("g")
            .attr("class", "y axis");

        this.context.append("g")
            .attr("class", "x brush")
            .call(this.brush)
            .selectAll("rect")
            .attr("y", 1)
            .attr("height", this.heightBrush - 1);
        
        this.focus.append("g")
            .attr("class", "data-lines");
        
        this.focus.append("g")
            .attr("class", "data-range");
        
        this.context.append("g")
            .attr("class", "data-lines");
        
        this.context.append("g")
            .attr("class", "data-range");
    }
    
    private onBrushed() {
        this.x
            .domain(this.brush.empty() ? this.xBrush.domain() : this.brush.extent() as any);
        
        this.zoom.x(this.x as any);
        
        this.draw(false, false);
    }
    
    private draw(dataUpdated: boolean, resetXScale: boolean) {
        const data = dataUpdated ? this.getDataToPlot() : undefined;
        
        const areaGenerator = <T>(d: graphSeries<dataRangePoint>, line: d3.svg.Line<dataPoint>) => {
            const mappedPoints = d.points.map(point => (line([{
                    date: point.from,
                    value: 0
                }, {
                    date: point.from,
                    value: point.value
                }, {
                    date: point.to,
                    value: point.value
                }, {
                    date: point.to,
                    value: 0
                }])
            ));

            return mappedPoints.join(" ");
        };

        if (dataUpdated) {
            if (data.pointSeries.length || data.rangeSeries.length) {
                const extents = timeSeriesDetails.computeExtents(data);
                const paddedExtents = timeSeriesDetails.paddingExtents(extents, 0.02);
                const { minX, maxX, minY, maxY } = paddedExtents;

                if (resetXScale) {
                    this.x.domain([minX, maxX]);
                }
                this.y.domain([minY, maxY]);
            } else {
                //TODO: show info that view is empty
                const now = new Date();
                this.x.domain([now, now]);
                this.y.domain([0, 0]);
            }

            if (resetXScale) {
                this.xBrush.domain(this.x.domain());
            }
            this.yBrush.domain(this.y.domain());
            this.zoom.x(this.x as any);
        }

        this.focus.select(".x.axis")
            .call(this.xAxis);

        this.focus.select(".y.axis")
            .call(this.yAxis);
        
        if (dataUpdated) {
            // areas should go beneath lines
            const focusAreas = this.focus
                .select(".data-range")
                .selectAll(".area")
                .data(data.rangeSeries);

            focusAreas.enter()
                .append("path")
                .attr("class", d => "area " + this.colorClassRangeScale(d.uniqueId));

            focusAreas.exit()
                .remove();
            
            const contextAreas = this.context
                .select(".data-range")
                .selectAll(".area")
                .data(data.rangeSeries);
            
            contextAreas.exit()
                .remove();
            
            contextAreas
                .attr("d", d => areaGenerator(d, this.lineBrush));
            
            contextAreas
                .enter()
                .append("path")
                .attr("class", d => "area " + this.colorClassPointScale(d.uniqueId))
                .attr("d", d => areaGenerator(d, this.lineBrush));
            
            // and draw lines above
            const focusLines = this.focus
                .select(".data-lines")
                .selectAll(".line")
                .data(data.pointSeries);

            focusLines.enter()
                .append("path")
                .attr("class", d => "line " + this.colorClassPointScale(d.uniqueId));

            focusLines.exit()
                .remove();
            
            const contextLines = this.context
                .select(".data-lines")
                .selectAll(".line")
                .data(data.pointSeries);
            
            contextLines.exit()
                .remove();

            contextLines
                .attr("d", d => this.lineBrush(d.points));
            
            contextLines
                .enter()
                .append("path")
                .attr("class", d => "line " + this.colorClassPointScale(d.uniqueId))
                .attr("d", d => this.lineBrush(d.points));
        }

        this.focus
            .select(".data-lines")
            .selectAll<graphSeries<dataPoint>>(".line")
            .attr("d", d => this.line(d.points));
        
        this.focus
            .select(".data-range")
            .selectAll<graphSeries<dataRangePoint>>(".area")
            .attr("d", d => areaGenerator(d, this.line));
        
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
        const pointsExtents = timeSeriesDetails.computePointExtents(data.pointSeries);
        const rangeExtents = timeSeriesDetails.computeRangeExtents(data.rangeSeries);
        
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

export = timeSeriesDetails;
