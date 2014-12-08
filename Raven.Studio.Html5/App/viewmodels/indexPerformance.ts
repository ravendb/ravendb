/// <reference path="../../Scripts/typings/d3/nvd3.d.ts" />
/// <reference path="../../Scripts/typings/d3/d3.d.ts" />
/// <reference path="../../Scripts/typings/bootstrap.multiselect/bootstrap.multiselect.d.ts" />


import shell = require("viewmodels/shell");
import viewModelBase = require("viewmodels/viewModelBase");
import getIndexingBatchStatsCommand = require("commands/getIndexingBatchStatsCommand");
import d3 = require('d3/d3');
import nv = require('nvd3');
import changeSubscription = require('models/changeSubscription');

class dateRange {
    constructor(public start: Date, public end: Date) {
    }
}
class timeGap {
    constructor(public position: number, public timespan: number) {
    }
}

class gapFinder {

    paddingBetweenGaps = 30;

    domain: Date[] = [];
    range: number[] = [];
    gapsPositions: timeGap[] = [];

    minTime: Date;
    maxTime: Date;

    totalWidth: number = 0;

    constructor(private dateRanges: dateRange[], private pixelsPerSecond: number, private minGapTime: number) {
        this.computeGaps(minGapTime);
        if (this.gapsPositions.length > 0) {
            this.totalWidth -= this.paddingBetweenGaps;
        }
    }

    public constructScale() {
        if (this.domain.length == 0) {
            return d3.time.scale();
        } else {
            return d3.time.scale()
                .range(this.range)
                .domain(this.domain);
        }
    }

    private pushRegion(region: dateRange) {
        this.domain.push(region.start);
        this.domain.push(region.end);

        if (this.totalWidth > 0) {
            var lastEnd = this.domain[this.domain.length - 3].getTime();
            var gap = new timeGap(this.totalWidth - this.paddingBetweenGaps / 2, region.start.getTime() - lastEnd);
            this.gapsPositions.push(gap);
        }
        
        this.range.push(this.totalWidth);
        var regionWidth = (region.end.getTime() - region.start.getTime()) / 1000 * this.pixelsPerSecond;
        this.range.push(this.totalWidth + regionWidth);
        this.totalWidth += regionWidth + this.paddingBetweenGaps;
    }

    private computeGaps(minGapTime: number) {
        if (this.dateRanges.length > 0) {
            var s = this.dateRanges[0].start;
            var e = this.dateRanges[0].end;
            this.minTime = s;
            this.maxTime = e;
            for (var i = 1; i < this.dateRanges.length; i++) {
                var newRange = this.dateRanges[i];

                if (this.minTime > newRange.start) {
                    this.minTime = newRange.start;
                }
                if (this.maxTime < newRange.end) {
                    this.maxTime = newRange.end;
                }

                if (newRange.start.getTime() > e.getTime() + minGapTime) {
                    this.pushRegion(new dateRange(s, e));
                    s = newRange.start;
                }
                e = newRange.end;
            }
            this.pushRegion(new dateRange(s, e));
        }
    }
}

class metrics extends viewModelBase { 

    static colors = { linq: '#004080', load: '#ffff80', write: '#00ff00', flush: '#9f0000' };

    static minGapTime = 1000 * 10;

    yBarHeight = 40;
    yBarMargin = 10;
    
    jsonData: indexingBatchInfoDto[] = [];
    rawJsonData: indexingBatchInfoDto[] = [];
    allIndexNames = ko.observableArray<string>();
    selectedIndexNames = ko.observableArray<string>();

    color = d3.scale.category10();
    margin = { top: 40, right: 20, bottom: 40, left: 200 };

    pixelsPerSecond = 100;

    width: number;

    isoFormat = d3.time.format.iso;
    xTickFormat = d3.time.format("%H:%M:%S");	

    xScale: D3.Scale.TimeScale;
    yScale: D3.Scale.OrdinalScale;
    xAxis: D3.Svg.Axis;
    yAxis: D3.Svg.Axis;
    svg: D3.Selection;

    private refreshGraphObservable = ko.observable<number>();
    private refreshSubscription: KnockoutSubscription;

    fetchJsonData() {
        return new getIndexingBatchStatsCommand(this.activeDatabase()).execute();
    }

    attached() {
        this.createKeyboardShortcut("esc", nv.tooltip.cleanup, "body");
        $("#metricsContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        $("#metricsContainer").scroll(() => this.graphScrolled());
        this.refresh();
        this.refreshSubscription = this.refreshGraphObservable.throttle(5000).subscribe((e) => this.refresh());
        this.selectedIndexNames.subscribe((v) => {
            this.filterJsonData();
            nv.tooltip.cleanup();
            this.redrawGraph();
        });
        $("#visibleIndexesSelector").multiselect();
    }

    createNotifications(): Array<changeSubscription> {
        return [shell.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e))];
    }

    processIndexEvent(e: indexChangeNotificationDto) {
        if (e.Type == "MapCompleted" || e.Type == "ReduceCompleted") {
            this.refreshGraphObservable(new Date().getTime());
        }
    }

    filterJsonData() {
        this.jsonData = [];
        var selectedIndexes = this.selectedIndexNames();
        this.rawJsonData.forEach(rawData => {
            var filteredStats = rawData.PerfStats.filter(p => selectedIndexes.contains(p.indexName));
            if (filteredStats.length > 0) {
                var rawCopy: indexingBatchInfoDto = $.extend(false, {}, rawData);
                rawCopy.PerfStats = filteredStats;
                this.jsonData.push(rawCopy);
            }
        });
    }

    refresh() {
        return this.fetchJsonData().done((data) => {
            this.rawJsonData = this.mergeJsonData(this.rawJsonData, data);

            var indexes = this.findIndexNames(this.rawJsonData);
            var oldAllIndexes = this.allIndexNames();
            var newIndexes = indexes.filter(i => !oldAllIndexes.contains(i));

            this.allIndexNames.pushAll(newIndexes);

            // this will filterJsonData and redrawGraph
            this.selectedIndexNames(this.selectedIndexNames().concat(newIndexes));
            // refresh multiselect widget:
            $("#visibleIndexesSelector").multiselect('rebuild');
        });
    }

    private mergeJsonData(currentData: indexingBatchInfoDto[], incomingData: indexingBatchInfoDto[]) {
        // create lookup map to avoid O(n^2) 
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.StartedAt, i);
        });

        incomingData.forEach(d => {
            if (dateLookup.has(d.StartedAt)) {
                var index = dateLookup.get(d.StartedAt);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });
        return currentData;
    }

    graphScrolled() {
        var leftScroll = $("#metricsContainer").scrollLeft();
        var self = this;
        this.svg.select('.y.axis')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('#dataClip rect')
            .attr('x', leftScroll);
        nv.tooltip.cleanup();
    }

    redrawGraph() {
        var self = this;

        this.width = $("#metricsContainer").width();

        // compute dates extents
        var gapsFinder = new gapFinder(
            this.jsonData.map(
                j => new dateRange(j.StartedAtDate, new Date(j.StartedAtDate.getTime() + j.TotalDurationMs))
                ), self.pixelsPerSecond, metrics.minGapTime);

        self.xScale = gapsFinder.constructScale();

        var totalWidthWithMargins = gapsFinder.totalWidth
            + self.margin.left
            + self.margin.right
            + 10; // add few more extra pixels
        var totalHeight =
            self.selectedIndexNames().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.top
            + self.margin.bottom;
        this.svg = d3.select("#indexPerformanceGraph");

        (this.svg.select('defs').node() !== null ? <any>this.svg.transition() : <any>this.svg)
            .attr("width", totalWidthWithMargins)
            .attr("height", totalHeight)
            .style('height', totalHeight + 'px')
            .style('width', totalWidthWithMargins + 'px')
            .attr("viewBox", "0 0 " + totalWidthWithMargins + " " + totalHeight);

        this.svg.selectAll('.main_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        $("#metricsContainer").css('overflow-x', totalWidthWithMargins > this.width ? 'scroll' : 'hidden');

        var defs = this.svg
            .selectAll('defs')
            .data([this.jsonData])
            .enter()
            .append('defs');

        defs.append('clipPath')
            .attr('id', 'dataClip')
            .append('rect')
            .attr('x', 0)
            .attr('y', -30)
            .attr('width', 1200000)
            .attr('height', 50000);

        defs.append('pattern')
            .attr('id', 'hash')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 6)
            .attr('height', 6)
            .attr('patternUnits', 'userSpaceOnUse')
            .append('path')
            .attr('d', 'M 0 0 L 6 6 M 0 6 L 6 0')
            .attr('stroke', 'grey')
            .attr('fill', 'transparent')
            .attr('stroke-width', 0.25)
            .attr('stroke-linecap', 'square')
            .attr('stroke-linejoin', 'miter');

        var svgEnter = this.svg
            .selectAll(".main_group")
            .data([this.jsonData]).enter();

        var mainGroup = svgEnter.append('g')
            .attr('class', 'main_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        mainGroup.append('g')
            .attr('class', 'batches');

        mainGroup.append('g')
            .attr('class', 'ops');

        mainGroup.append('g')
            .attr('class', 'gaps');

        var controllsEnter = this.svg
            .selectAll(".controlls")
            .data([this.jsonData]).enter()
            .append("g")
            .attr('class', 'controlls')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.updateXAxis(controllsEnter, gapsFinder.totalWidth);
        this.updateYAxis(controllsEnter);
        this.updateBatchesRanges();
        this.updateOperations();
        this.updateGaps(gapsFinder.gapsPositions);
    }

    private updateXAxis(controllsEnter, totalWidth:number) {
        var self = this;
        controllsEnter.append("g")
            .attr("class", "x axis")
            .attr('clip-path', "url(#dataClip)");

        var t = d3.scale.linear()
            .domain([0, totalWidth])
            .ticks(Math.ceil(totalWidth / 100)).map(y => self.xScale.invert(y));

        self.xAxis = d3.svg.axis()
            .scale(self.xScale)
            .orient("top")
            .tickValues(t)
            .tickSize(10)
            .tickFormat(self.xTickFormat);

        this.svg.select(".x.axis")
            .attr("transform", "translate(0,0)")
            .transition()
            .call(self.xAxis);
    }

    private updateYAxis(controllsEnter) {
        var self = this;
        controllsEnter.append('g')
            .attr('class', 'y axis')
            .attr("transform", "translate(0,0)");

        var indexCount = self.selectedIndexNames().length;

        self.yScale = d3.scale.ordinal()
            .domain(self.selectedIndexNames())
            .rangeBands([0, indexCount * (self.yBarHeight + self.yBarMargin * 2)]);

        self.yAxis = d3.svg.axis()
            .scale(self.yScale)
            .orient("left");

        self.svg.select(".y.axis")
            .transition()
            .call(self.yAxis);
    }

    // uses optimalization which assumes that both start and start + size falls into continuous region
    private xScaleExtent(size: number) {
        var self = this;
        return size / 1000 * self.pixelsPerSecond;
    }

    private updateBatchesRanges() {
        var self = this;
        var batches = self.svg.select(".batches")
            .selectAll(".batchRange")
            .data(self.jsonData, d => d.StartedAt);

        batches.exit().remove();

        batches.select('rect') 
            .transition()
                .attr('x', (d: indexingBatchInfoDto) => self.xScale(d.StartedAtDate))
                .attr('y', (d: indexingBatchInfoDto) => d3.min(d.PerfStats, v => self.yScale(v.indexName)))
                .attr('width', (d: indexingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs))
                .attr('height', (d: indexingBatchInfoDto) => {
                    var extent = d3.extent(d.PerfStats, v => self.yScale(v.indexName));
                    return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
                });

        batches.enter()
            .append('g')
                .attr('class', 'batchRange')
                .on('click', function (d) { return self.batchInfoClicked(d, this); })
            .append('rect')
                .attr('x', (d: indexingBatchInfoDto) => self.xScale(d.StartedAtDate))
                .attr('y', (d: indexingBatchInfoDto) => d3.min(d.PerfStats, v => self.yScale(v.indexName)))
                .attr('width', 0)
                .attr('height', (d: indexingBatchInfoDto) => {
                    var extent = d3.extent(d.PerfStats, v => self.yScale(v.indexName));
                    return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
                })
                .style("fill", d => self.color(d.StartedAt))
             .transition()
                .attr('width', (d: indexingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));
    }

    private updateOperations() {
        var self = this;
        var batches = self.svg.select(".ops").selectAll(".opGroup")
            .data(self.jsonData, d => d.StartedAt);

        batches.exit().remove();

        var enteringOpsGroups = batches
            .enter()
            .append('g')
            .attr('class', 'opGroup');

        var op = batches.selectAll('.op')
            .data((d: indexingBatchInfoDto) => d.PerfStats, d => d.indexName);

        op.exit().remove();

        var opTransition = 
        op.
            transition()
            .attr("transform",
                (d: indexNameAndPerformanceStats) =>
                    "translate(" + self.xScale(self.isoFormat.parse(d.stats.Started)) + "," + self.yScale(d.indexName) + ")");

        opTransition.select('.main_bar')
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(d.stats.DurationMilliseconds));

        opTransition.select('.linq_bar')
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)));

        opTransition.select('.load_bar')
            .attr('x', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)))
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LoadDocumentDurationMs, 0)));

        opTransition.select('.write_bar')
            .attr('x', (d: indexNameAndPerformanceStats) =>
                self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0) + Math.max(d.stats.LoadDocumentDurationMs, 0)))
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.WritingDocumentsToLuceneDurationMs, 0)));
            
        opTransition.select('.flush_bar')
            .attr('x', (d: indexNameAndPerformanceStats) =>
                self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)
                    + Math.max(d.stats.LoadDocumentDurationMs, 0)
                    + Math.max(d.stats.WritingDocumentsToLuceneDurationMs, 0)))
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.FlushToDiskDurationMs, 0)));

        var enteringOps = op.enter()
            .append('g')
            .attr('class', 'op')
            .on('click', function (d) { return self.indexStatClicked(d, this); })
            .attr("transform",
            (d: indexNameAndPerformanceStats) =>
                "translate(" + self.xScale(self.isoFormat.parse(d.stats.Started)) + "," + self.yScale(d.indexName) + ")");

        enteringOps.append('rect')
                .attr('class', 'main_bar')
                .attr('x', 0)
                .attr('y', self.yBarMargin)
                .attr('width', 0)
                .attr('height', self.yBarHeight)
                .style('fill', function () {
                    var d = d3.select(this.parentNode.parentNode).datum().StartedAt;
                    return self.color(d);
                })
            .transition()
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(d.stats.DurationMilliseconds));

        enteringOps.append('rect')
            .attr('class', 'linq_bar')
            .attr('x', 0)
            .attr('y', self.yBarMargin + 3)
            .attr('width', 0)
            .attr('height', self.yBarHeight - 6)
            .style('fill', metrics.colors.linq)
            .transition()
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)));

        enteringOps.append('rect')
            .attr('class', 'load_bar')
            .attr('x', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)))
            .attr('y', self.yBarMargin + 3)
            .attr('width', 0)
            .attr('height', self.yBarHeight - 6)
            .style('fill', metrics.colors.load)
            .transition()
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.LoadDocumentDurationMs, 0)));

        enteringOps.append('rect')
            .attr('class', 'write_bar')
            .attr('x', (d: indexNameAndPerformanceStats) =>
                self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0) + Math.max(d.stats.LoadDocumentDurationMs, 0)))
            .attr('y', self.yBarMargin + 3)
            .attr('width', 0)
            .attr('height', self.yBarHeight - 6)
            .style('fill', metrics.colors.write)
            .transition()
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.WritingDocumentsToLuceneDurationMs, 0)));

        enteringOps.append('rect')
            .attr('class', 'flush_bar')
            .attr('x', (d: indexNameAndPerformanceStats) =>
                self.xScaleExtent(Math.max(d.stats.LinqExecutionDurationMs, 0)
                    + Math.max(d.stats.LoadDocumentDurationMs, 0)
                    + Math.max(d.stats.WritingDocumentsToLuceneDurationMs, 0)))
            .attr('y', self.yBarMargin + 3)
            .attr('width', 0)
            .attr('height', self.yBarHeight - 6)
            .style('fill', metrics.colors.flush)
            .transition()
            .attr('width', (d: indexNameAndPerformanceStats) => self.xScaleExtent(Math.max(d.stats.FlushToDiskDurationMs, 0)));

    }

    private updateGaps(gapsPositions: timeGap[]) {
        var self = this;
        var gaps = self.svg.select('.gaps').selectAll('.gap')
            .data(gapsPositions);

        gaps.exit().remove();

        var patternWidth = 20;

        gaps.select('text')
            .transition()
            .attr("y", d => d.position)
            .attr('x', - self.selectedIndexNames().length * (self.yBarHeight + self.yBarMargin * 2) / 2);

        gaps.select('rect')
            .transition()
                .attr('x', d => d.position - patternWidth / 2)
                .attr('height', self.selectedIndexNames().length * (self.yBarHeight + self.yBarMargin * 2));

        var enteringGaps = gaps.enter()
            .append('g')
            .attr('class', 'gap');

        enteringGaps.append('rect')
                .attr('x', d => d.position - patternWidth / 2)
                .attr('y', 0)
                .attr('width', patternWidth)
                .attr('height', 0)
                .style('fill', 'url(#hash)')
            .transition()
            .attr('height', self.selectedIndexNames().length * (self.yBarHeight + self.yBarMargin * 2));

        enteringGaps.append('text')
            .attr("transform", "rotate(-90)")
            .attr("y", d => d.position)
            .attr('x', - self.selectedIndexNames().length * (self.yBarHeight + self.yBarMargin * 2) / 2)
            .attr("dy", 5)
            .style("text-anchor", "middle")
            .text(d => self.timeSpanFormat(d.timespan));
    }

    private timeSpanFormat(milis: number) {
        var t = milis;
        t -= t % 1000;
        t /= 1000;
        var s = t % 60;
        t -= s;
        t /= 60;
        var m = t % 60;
        t -= m;
        t /= 60;
        var h = t;
        var f = d3.format("02d");
        return f(h) + ":" + f(m) + ":" + f(s);

    }

    private indexStatClicked(data: indexNameAndPerformanceStats, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'index-stat-template\' }"></div>'; 
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width / 2, offset.top], html, 'n', self.yBarHeight + 15, null, "selectable-tooltip");
        self.fillTooltip(data);
    }

    private batchInfoClicked(data: indexingBatchInfoDto, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'batch-info-template\' }"></div>'; 
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width / 2, offset.top], html, 'n', self.yBarHeight + 15, null, "selectable-tooltip");
        self.fillTooltip(data);
    }

    private fillTooltip(model: any) {
        var node = $(".nvtooltip")[0];
        ko.applyBindings({ data: model, tooltipClose:  nv.tooltip.cleanup }, node);
    }

    onWindowHeightChanged() {
        nv.tooltip.cleanup();
        this.width = $("#metricsContainer").width();

        $("#metricsContainer").css('overflow-x', $("#metricsContainer svg").width() > this.width ? 'scroll' : 'hidden');
    }

    detached() {
        super.detached();

        $("#metricsContainer").off('DynamicHeightSet');
        nv.tooltip.cleanup();
        if (this.refreshSubscription != null) {
            this.refreshSubscription.dispose();
        }
    }

    findIndexNames(jsonData: indexingBatchInfoDto[]) {
        var statsInline = d3.merge(jsonData.map((d) => d.PerfStats));
        var byKey = d3
            .nest()
            .key(d => d.indexName)
            .sortKeys(d3.ascending)
            .rollup(l => l.length)
            .entries(statsInline);
        return byKey.map(d => d.key);
    }

    zoomIn() {
        this.pixelsPerSecond += 10;
        nv.tooltip.cleanup();
        this.redrawGraph();
    }

    zoomOut() {
        if (this.pixelsPerSecond > 10) {
            this.pixelsPerSecond -= 10;
        }
        nv.tooltip.cleanup();
        this.redrawGraph();
    }
}

export = metrics; 
