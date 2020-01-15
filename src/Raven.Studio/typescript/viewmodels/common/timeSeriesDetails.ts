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
    series: seriesData[]; 
}

interface seriesData {
    dataPoints: dataPoint[];
    color: string;
    name: string;
}

interface dataPoint {
    date: Date;
    value: number;
}

type displayMode = "raw" | "aggregate";

class graphSeries {
    onChange: () => void;
    uniqueId = _.uniqueId("ts-series");
    visible = ko.observable<boolean>(true);
    name: string;
    color: string;
    points: dataPoint[];
    
    constructor(name: string, points: dataPoint[], onChange: () => void) {
        this.name = name;
        this.onChange = onChange;
        this.points = points;
        
        this.visible.subscribe(() => this.onChange());
    }

    getSeriesData(): seriesData {
        return {
            color: this.color,
            name: this.name,
            dataPoints: this.points
        }
    }
}

abstract class timeSeriesContainer {
    sourceDocument: document;
    path: string;
    value: timeSeriesQueryResultDto;
    abstract type: timeSeriesResultType;
    series = ko.observableArray<graphSeries>();
    onChange: () => void;
    
    protected constructor(item: timeSeriesItem, onChange: () => void) {
        this.sourceDocument = item.document;
        this.path = item.path;
        this.value = (item.document as any)[item.path] as timeSeriesQueryResultDto;
        this.onChange = onChange;
        
        //TODO: assign colors!
    }
    
    get sectionName() {
        return this.sourceDocument.getId() + " - " + this.path;
    }
    
    getSeriesData(): seriesData[] {
        return this.series()
            .filter(x => x.visible())
            .map(x => x.getSeriesData());
    }
    
    static for(item: timeSeriesItem, onChange: () => void) {
        const value = (item.document as any)[item.path] as timeSeriesQueryResultDto;
        const type = timeSeriesQueryResult.detectResultType(value);
        switch (type) {
            case "grouped":
                return new groupedTimeSeriesContainer(item, onChange);
            case "raw":
                return new rawTimeSeriesContainer(item, onChange);
        }
    }
}

class groupedTimeSeriesContainer extends timeSeriesContainer {
    type: timeSeriesResultType = "grouped"; 
    
    constructor(item: timeSeriesItem, onChange: () => void) {
        super(item, onChange);

        const groupedValues = this.value.Results as Array<timeSeriesQueryGroupedItemResultDto>;
        const allKeys = Object.keys(groupedValues[0]);
        const seriesNames = _.without(allKeys, "From", "To", "Count");
        this.series(seriesNames.map(x => new graphSeries(x, [], onChange)));//TODO:
    }
}

class rawTimeSeriesContainer extends timeSeriesContainer {
    type: timeSeriesResultType = "raw";

    displayMode = ko.observable<displayMode>("raw");
    
    rawSeries = ko.observableArray<graphSeries>([]);
    aggregateSeries = ko.observableArray<graphSeries>([]);
    
    aggregation = {
        min: ko.observable<boolean>(true),
        max: ko.observable<boolean>(true),
        avg: ko.observable<boolean>(true)
    };
    
    constructor(item: timeSeriesItem, onChange: () => void) {
        super(item, onChange);

        this.prepareSeries();
        this.setEffectiveSeries();
        
        this.displayMode.subscribe(() => {
            this.setEffectiveSeries();
            this.onChange();
        });
    }

    selectDisplayMode(mode: displayMode) {
        this.displayMode(mode);
    }
    
    setEffectiveSeries() {
        this.series(this.displayMode() === "raw" ? this.rawSeries() : this.aggregateSeries());
    }
    
    private prepareSeries() {
        const rawValues = this.value.Results as Array<timeSeriesRawItemResultDto>;
        const valuesCount = rawValues[0].Values.length;
        const seriesName = _.range(valuesCount).map((_, idx) => "Value #" + (idx + 1));
        
        const datePoints = rawValues.map(x => moment.utc(x.Timestamp).toDate());
        
        this.rawSeries(seriesName.map((name, seriesNameIdx) => {
            const dataPoints: dataPoint[] = rawValues.map((v, valuesIdx) => ({
                value: v.Values[seriesNameIdx],
                date: datePoints[valuesIdx]
            }));
            return new graphSeries(name, dataPoints, this.onChange);
        }));

        const aggregateSeriesName = ["Minimum", "Maximum", "Average"];
        this.aggregateSeries(aggregateSeriesName.map(x => new graphSeries(x, [], this.onChange))); //TODO:
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
    timeSeries: timeSeriesContainer[];

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
    private area: d3.svg.Line<dataPoint>; //TODO: rename to line!
    private areaBrush: d3.svg.Line<dataPoint>;
    
    private zoom: d3.behavior.Zoom<void>;
    private rect: d3.Selection<any>;
    
    constructor(timeSeries: Array<timeSeriesItem>, initialMode: viewMode = "plot") {
        super();
        
        this.timeSeries = timeSeries.map(x => timeSeriesContainer.for(x, () => this.draw(true, false)));
        this.mode(initialMode);
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
        
        this.area = d3.svg.line<dataPoint>()
            .x(x => this.x(x.date))
            .y(x => this.y(x.value));
        
        this.areaBrush = d3.svg.line<dataPoint>()
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
    }
    
    private onBrushed() {
        this.x
            .domain(this.brush.empty() ? this.xBrush.domain() : this.brush.extent() as any);
        
        this.zoom.x(this.x as any);
        
        this.draw(false, false);
    }
    
    private draw(dataUpdated: boolean, resetXScale: boolean) { //TODO: initScale add default value = false
        const data = dataUpdated ? this.getDataToPlot() : undefined;
        
        if (dataUpdated) {
            if (data.series.length) {
                const { minX, maxX, minY, maxY } = timeSeriesDetails.computeExtends(data);

                this.x.domain([minX, maxX]);
                this.y.domain([minY, maxY]); //TODO: scale to zero ?
            } else {
                //TODO: show info that view is empty
                const now = new Date();
                this.x.domain([now, now]);
                this.y.domain([0, 0]);
            }

            this.xBrush.domain(this.x.domain());
            this.yBrush.domain(this.y.domain());
            this.zoom.x(this.x as any);
        }

        this.focus.select(".x.axis")
            .call(this.xAxis);

        this.focus.select(".y.axis")
            .call(this.yAxis);
        
        if (dataUpdated) {
            const focusLines = this.focus
                .selectAll(".line")
                .data(data.series);

            focusLines.enter()
                .append("path")
                .attr("class", "line");

            focusLines.exit()
                .remove();
            
            const contextLines = this.context
                .selectAll(".line")
                .data(data.series);
            
            contextLines.exit()
                .remove();

            contextLines
                .attr("d", d => this.areaBrush(d.dataPoints));
            
            contextLines
                .enter()
                .append("path")
                .attr("class", "line")
                .attr("d", d => this.areaBrush(d.dataPoints));
        }

        this.focus
            .selectAll<seriesData>(".line")
            .attr("d", d => this.area(d.dataPoints));
            
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
    
    private static computeExtends(data: graphData) {
        const nonEmptySeries = data.series.filter(x => x.dataPoints.length);
        
        const minX = d3.min(nonEmptySeries.map(x => x.dataPoints[0].date));
        const maxX = d3.max(nonEmptySeries.map(x => x.dataPoints[x.dataPoints.length - 1].date));

        const yExtendsList = nonEmptySeries.map(x => d3.extent(x.dataPoints.map(y => y.value)));
        const minY = d3.min(yExtendsList.map(x => x[0]));
        const maxY = d3.max(yExtendsList.map(x => x[1]));
        
        return {
            minX, maxX, 
            minY, maxY
        };
    }
    
    getDataToPlot(): graphData {
        const result: graphData = {
            series: []
        };
        
        this.timeSeries.forEach(item => {
            result.series.push(...item.getSeriesData());
        });
        
        return result;
    }
}

export = timeSeriesDetails;
