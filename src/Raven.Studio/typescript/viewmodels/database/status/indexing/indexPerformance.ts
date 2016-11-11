import viewModelBase = require("viewmodels/viewModelBase");
import app = require("durandal/app");
import tempStatDialog = require("viewmodels/database/status/indexing/tempStatDialog");
import getIndexesPerformance = require("commands/database/debug/getIndexesPerformance");
import fileDownloader = require("common/fileDownloader");
import graphHelper = require("common/helpers/graph/graphHelper");
import d3 = require("d3");

class metrics extends viewModelBase { 

    static readonly brushSectionHeight = 40;
    static readonly trackHeight = 16; // height used for callstack item
    static readonly stackPadding = 1; // space between call stacks
    static readonly trackPadding = 3; // top / bottom padding between different tracks

    private data: Raven.Client.Data.Indexes.IndexPerformanceStats[] = [];
    private timeRange: [Date, Date];
    private totalWidth = 700; //TODO: use dynamic value + bind on window resize
    private totalHeight = 700; //TODO: use dynamic value + bind on windows resize

    private indexNamesAndRecursion: Map<string, number>;
    private expandedIndexes = new Set<string>();

    private isoParser = d3.time.format.iso;
    private canvas: d3.Selection<any>;
    private svg: d3.Selection<any>; // spans to canvas size (to provide brush + zoom/pan features)
    private brush: d3.svg.Brush<number>;
    private xBrushNumericScale: d3.scale.Linear<number, number>;
    private xNumericScale: d3.scale.Linear<number, number>;
    private brushSection: HTMLCanvasElement; // virtual canvas for brush section
    private brushContainer: d3.Selection<any>;
    private zoom: d3.behavior.Zoom<any>;

    activate(args: any): JQueryPromise<any> {
        super.activate(args);

        return new getIndexesPerformance(this.activeDatabase())
            .execute()
            .done(result => this.data = result)
    }

    attached() {
        super.attached();

        this.initCanvas();
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
            .on("zoom", () => this.onZoom());

        this.svg
            .append("svg:rect")
            .attr("class", "pane")
            .attr("width", this.totalWidth)
            .attr("height", this.totalHeight - metrics.brushSectionHeight)
            .attr("transform", "translate(" + 0 + "," + metrics.brushSectionHeight + ")")
            .call(this.zoom);
    }

    private draw() {
        this.prepareBrushSection();
        this.prepareMainSection();

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");
        context.drawImage(this.brushSection, 0, 0);

        this.drawMainSection();
    }

    private prepareBrushSection() {
        const timeRanges = this.extractTimeRanges();
        const collapsedTimeRanges = graphHelper.collapseTimeRanges(timeRanges);
        this.timeRange = graphHelper.timeRangeFromSortedRanges(collapsedTimeRanges);

        this.brushSection = document.createElement("canvas");
        this.brushSection.width = this.totalWidth;
        this.brushSection.height = metrics.brushSectionHeight;

        const context = this.brushSection.getContext("2d");

        const xBrushScale = d3.time.scale<number>()
            .range([0, this.totalWidth])
            .domain(this.timeRange);

        this.drawBrushXaxis(context, xBrushScale);

        context.fillStyle = "#e2ebfe";
        context.strokeStyle = "#6e9cf8";
        context.lineWidth = 1;

        for (var i = 0; i < collapsedTimeRanges.length; i++) {
            const currentRange = collapsedTimeRanges[i];
            context.fillRect(xBrushScale(currentRange[0]), 18, xBrushScale(currentRange[1]), 28);
            context.strokeRect(xBrushScale(currentRange[0]), 18, xBrushScale(currentRange[1]), 28);
        }

        this.prepareBrush();
    }

    private prepareBrush() {
        this.brushContainer = this.svg
            .append("g")
            .attr("class", "x brush");

        this.brushContainer
            .call(this.brush)
            .selectAll("rect")
            .attr("y", 1)
            .attr("height", metrics.brushSectionHeight - 2);
    }

    private prepareMainSection() {
        //TODO: find index names, and max recurrsion level, compute scale for drawing indexes, init cache strucute to store info about expanded collapsed sections
        this.indexNamesAndRecursion = this.findIndexNamesAndMaxRecursionLevel();

        //TODO: include support for expanding tracks

        console.log(this.indexNamesAndRecursion);
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

    private drawBrushXaxis(context: CanvasRenderingContext2D, xBrushScale: d3.time.Scale<number, number>) { //TODO: extract this to utils? 
        const tickCount = Math.floor(this.totalWidth / 300);
        const tickSize = 6;
        const ticks = xBrushScale.ticks(tickCount);
        const tickFormat = xBrushScale.tickFormat(tickCount);

        context.beginPath();
        context.moveTo(0, tickSize);
        context.lineTo(0, 0);
        context.lineTo(this.totalWidth - 1, 0);
        context.lineTo(this.totalWidth - 1, tickSize);

        ticks.forEach(x => {
            context.moveTo(xBrushScale(x), 0);
            context.lineTo(xBrushScale(x), tickSize);
        });
        context.strokeStyle = "white";
        context.stroke();

        context.textAlign = "center";
        context.textBaseline = "top";
        context.fillStyle = "white";
        ticks.forEach(x => {
            context.fillText(tickFormat(x), xBrushScale(x), tickSize);
        });
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
        //console.log(this.xNumericScale.domain());
        //TODO: console.log(extent.map(x => this.xBrushScale.invert(x)));
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
        const xScale = d3.time.scale<number>()
            .range([0, this.totalWidth])
            .domain(this.timeRange);

        const visibleTimeFrame = this.xNumericScale.domain().map(x => xScale.invert(x)) as [Date, Date];
        console.log(visibleTimeFrame);

        const canvas = this.canvas.node() as HTMLCanvasElement;
        const context = canvas.getContext("2d");

        context.save();
        try {
            context.translate(0, metrics.brushSectionHeight);
            context.rect(0, 0, this.totalWidth, this.totalHeight - metrics.brushSectionHeight); //TODO: make sure it is needed
            context.clip();



        } finally {
            context.restore();
        }

        
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

    private dataImported(result: string) { //TODO:
        const json = JSON.parse(result) as {
            data: Raven.Client.Data.Indexes.IndexPerformanceStats[];
        };

        this.data = json.data;

        $("#indexPerformanceGraph").empty();
        //TODO: this.draw();
    }

    exportAsJson() { //TODO:
        fileDownloader.downloadAsJson({
            data: this.data
        }, "perf.json", "perf");
    }
}

export = metrics; 
 
