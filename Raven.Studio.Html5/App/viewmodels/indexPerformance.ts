/// <reference path="../../Scripts/typings/bootstrap.multiselect/bootstrap.multiselect.d.ts" />


import shell = require("viewmodels/shell");
import viewModelBase = require("viewmodels/viewModelBase");
import getIndexingBatchStatsCommand = require("commands/getIndexingBatchStatsCommand");
import getReducingBatchStatsCommand = require("commands/getReducingBatchStatsCommand");
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


    static map_bar_names =
        ['load_bar',
        'linq_map_bar', 'linq_reduce_bar',
        'l_delete_bar', 'l_convert_bar', 'l_add_bar', 'l_flush_bar', 'l_recreate_bar',
        'map_del_bar', 'map_convert_bar', 'map_put_bar', 'map_schedule_bar', 'map_commit_bar'];

    static map_bar_colors = [
        '#17becf',
        '#1f77b4', '#ff7f0e',
        '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2',
        '#aec7e8', '#e0e5a3', '#98df8a', '#ffbb78', '#ff9896'];

    static reduce_bar_names = 
        ['linq_map_bar', 'linq_reduce_bar',
        'l_delete_bar', 'l_convert_bar', 'l_add_bar', 'l_flush_bar', 'l_recreate_bar',
        'red_get_items_bar', 'red_delete_bar', 'red_schedule_bar', 'red_get_mapped_bar', 'red_remove_bar', 'red_commit_bar'];

    static reduce_bar_colors = [
        '#1f77b4', '#ff7f0e',
        '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2',
        '#c5b0d5', '#c49c94', '#f7b6d2', '#bcbd22', '#dbdb8d', '#9edae5'];

    static minGapTime = 1000 * 10;

    yBarHeight = 40;
    yBarMargin = 10;

    reduceGroupOffset = 0;
    
    mapJsonData: indexingBatchInfoDto[] = [];
    rawMapJsonData: indexingBatchInfoDto[] = [];

    reduceJsonData: reducingBatchInfoDto[] = [];
    rawReduceJsonData: reducingBatchInfoDto[] = [];

    mapAllIndexNames = ko.observableArray<string>();
    reduceAllIndexNames = ko.observableArray<string>();
    allIndexNames = ko.computed(() => d3.set(this.mapAllIndexNames().concat(this.reduceAllIndexNames())).values());
    selectedIndexNames = ko.observableArray<string>();
    selectedMapIndexNames = ko.computed(() => this.selectedIndexNames().filter(x => this.mapAllIndexNames().contains(x)));
    selectedReduceIndexes = ko.computed(() => this.selectedIndexNames().filter(x => this.reduceAllIndexNames().contains(x)));

    static batchesColors = ['#ececec', '#c2c2c2', '#959595'];

    color = d3.scale.ordinal().range(metrics.batchesColors);
    margin = { top: 40, right: 20, bottom: 40, left: 200, between: 10 };

    private pixelsPerSecond = ko.observable<number>(100);

    width: number;

    isoFormat = d3.time.format.iso;
    xTickFormat = d3.time.format("%H:%M:%S");	

    xScale: D3.Scale.TimeScale;
    yMapScale: D3.Scale.OrdinalScale;
    yReduceScale: D3.Scale.OrdinalScale;

    xAxis: D3.Svg.Axis;
    yMapAxis: D3.Svg.Axis;
    yReduceAxis: D3.Svg.Axis;

    svg: D3.Selection;

    private refreshGraphObservable = ko.observable<number>();
    private refreshSubscription: KnockoutSubscription;

    constructor() {
        super();
        this.pixelsPerSecond.throttle(100).subscribe((value) => {
            nv.tooltip.cleanup();
            this.redrawGraph();
        });
    }

    fetchMapJsonData() {
        return new getIndexingBatchStatsCommand(this.activeDatabase()).execute();
    }

    fetchReduceJsonData() {
        return new getReducingBatchStatsCommand(this.activeDatabase()).execute();
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
        this.mapJsonData = [];
        this.reduceJsonData = [];
        var selectedIndexes = this.selectedIndexNames();
        this.rawMapJsonData.forEach(rawData => {
            var filteredStats = rawData.PerfStats.filter(p => selectedIndexes.contains(p.indexName));
            if (filteredStats.length > 0) {
                var rawCopy: indexingBatchInfoDto = $.extend(false, {}, rawData);
                rawCopy.PerfStats = filteredStats;
                this.mapJsonData.push(rawCopy);
            }
        });
        this.rawReduceJsonData.forEach(rawData => {
            var filteredStats = rawData.PerfStats.filter(p => selectedIndexes.contains(p.indexName));
            if (filteredStats.length > 0) {
                var rawCopy: reducingBatchInfoDto = $.extend(false, {}, rawData);
                rawCopy.PerfStats = filteredStats; 
                this.reduceJsonData.push(rawCopy);
            }
        });
    }

    refresh() {
        var mapTask = this.fetchMapJsonData();
        var reduceTask = this.fetchReduceJsonData();

        $.when(mapTask, reduceTask)
            .done((mapResult, reduceResult) => {
                var oldAllIndexes = this.allIndexNames();

                this.rawMapJsonData = this.mergeMapJsonData(this.rawMapJsonData, mapResult);
                var mapIndexes = this.findIndexNames(this.rawMapJsonData);
                var oldMapAllIndexes = this.mapAllIndexNames();
                var newMapIndexes = mapIndexes.filter(i => !oldMapAllIndexes.contains(i));
                this.mapAllIndexNames.pushAll(newMapIndexes);

                this.rawReduceJsonData = this.mergeReduceJsonData(this.rawReduceJsonData, reduceResult);
                var reduceIndexes = this.findIndexNames(this.rawReduceJsonData);
                var oldReduceAllIndexes = this.reduceAllIndexNames();
                var newReduceIndexes = reduceIndexes.filter(i => !oldReduceAllIndexes.contains(i));
                this.reduceAllIndexNames.pushAll(newReduceIndexes);

                var newIndexes = d3.set(mapIndexes.concat(reduceIndexes)).values().filter(i => !oldAllIndexes.contains(i));

                // this will filterJsonData and redrawGraph
                this.selectedIndexNames(this.selectedIndexNames().concat(newIndexes));
                // refresh multiselect widget:
                $("#visibleIndexesSelector").multiselect('rebuild');
            });
    }

    private mergeMapJsonData(currentData: indexingBatchInfoDto[], incomingData: indexingBatchInfoDto[]) {
        // create lookup map to avoid O(n^2) 
        var self = this;
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.StartedAt, i);
        });

        incomingData.forEach(d => {

            d.PerfStats.forEach(self.computeMapCache);

            if (dateLookup.has(d.StartedAt)) {
                var index = dateLookup.get(d.StartedAt);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });
        return currentData;
    }

    private mergeReduceJsonData(currentData: reducingBatchInfoDto[], incomingData: reducingBatchInfoDto[]) {
        // create lookup map to avoid O(n^2) 
        var self = this;
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.StartedAt, i);
        });

        incomingData.forEach(d => {
            d.PerfStats.forEach(s => s.stats.LevelStats.forEach(self.computeReduceCache));

            if (dateLookup.has(d.StartedAt)) {
                var index = dateLookup.get(d.StartedAt);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });
        return currentData;
    }

    computeMapCache(input: indexNameAndMapPerformanceStats) {
        var currentOffset = 0;

        var timings = input.stats.Operations.map(o => o.DurationMs);

        for (var i = 0; i < timings.length; i++) {
            var currentWidth = Math.max(timings[i], 0);

            var op = input.stats.Operations[i];
            op.CacheWidth = currentWidth;
            op.CacheCumulativeSum = currentOffset;
            op.CacheIsSingleThread = "Name" in op;

            if (!op.CacheIsSingleThread) {
                var mtGroup = <parallelPefromanceStatsDto>op;
                for (var thread = 0; thread < mtGroup.BatchedOperations.length; thread++) {
                    var perThreadInfo = mtGroup.BatchedOperations[thread];
                    var currentPerThreadOffset = perThreadInfo.StartDelay;

                    for (var j = 0; j < perThreadInfo.Operations.length; j++) {
                        var innerOp = perThreadInfo.Operations[j];
                        var w = Math.max(innerOp.DurationMs, 0);
                        innerOp.CacheWidth = w;
                        innerOp.CacheCumulativeSum = currentPerThreadOffset;
                        innerOp.CacheIsSingleThread = true;
                        currentPerThreadOffset += w;
                    }
                }
            }

            currentOffset += currentWidth;
        }
    }

    computeReduceCache(input: reduceLevelPeformanceStatsDtoWithCache) {
        var widths = [];
        var sums = [];
        var currentOffset = 0;

        var timings = input.Operations.map(o => o.DurationMs);
        for (var i = 0; i < timings.length; i++) {
            var currentWidth = Math.max(timings[i], 0);
            widths.push(currentWidth);
            sums.push(currentOffset); 
            currentOffset += currentWidth;
        }

        input.widths = widths;
        input.cumulativeSums = sums;
    }

    graphScrolled() {
        var leftScroll = $("#metricsContainer").scrollLeft();
        var self = this;
        this.svg.select('.y.axis.map')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('.y.axis.reduce')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('#dataClip rect')
            .attr('x', leftScroll);

        this.svg.select('.left_texts')
            .attr("transform", "translate(" + leftScroll + ",0)");

        nv.tooltip.cleanup();
    }

    redrawGraph() {
        var self = this;

        this.width = $("#metricsContainer").width();

        // compute dates extents
        var mapDateRange = this.mapJsonData.map(
            j => new dateRange(j.StartedAtDate, new Date(j.StartedAtDate.getTime() + j.TotalDurationMs)));

        var reduceDateRange = this.reduceJsonData.map(
            j => new dateRange(j.StartedAtDate, new Date(j.StartedAtDate.getTime() + j.TotalDurationMs)));

        var mergedDateRange = mapDateRange.concat(reduceDateRange).sort((a, b) => a.start.getTime() - b.start.getTime());

        var gapsFinder = new gapFinder(mergedDateRange, self.pixelsPerSecond(), metrics.minGapTime);

        self.xScale = gapsFinder.constructScale();

        var totalWidthWithMargins = gapsFinder.totalWidth
            + self.margin.left
            + self.margin.right
            + 10; // add few more extra pixels
        var totalHeight =
            self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.between
            + self.selectedReduceIndexes().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.top
            + self.margin.bottom;
        this.svg = d3.select("#indexPerformanceGraph");

        (this.svg.select('defs').node() !== null ? <any>this.svg.transition() : <any>this.svg)
            .attr("width", totalWidthWithMargins)
            .attr("height", totalHeight)
            .style('height', totalHeight + 'px')
            .style('width', totalWidthWithMargins + 'px')
            .attr("viewBox", "0 0 " + totalWidthWithMargins + " " + totalHeight);

        self.reduceGroupOffset = self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2) + self.margin.between;

        this.svg.selectAll('.map_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.svg.selectAll('.reduce_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        $("#metricsContainer").css('overflow-x', totalWidthWithMargins > this.width ? 'scroll' : 'hidden');

        var defs = this.svg
            .selectAll('defs')
            .data([null])
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
            .selectAll(".map_group")
            .data([null]).enter();

        svgEnter.append('g')
            .attr('class', 'gaps')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")"); 

        var mapGroup = svgEnter.append('g')
            .attr('class', 'map_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        mapGroup.append('g')
            .attr('class', 'batches');

        mapGroup.append('g')
            .attr('class', 'ops');

        var reduceGroup = svgEnter.append('g')
            .attr('class', 'reduce_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        reduceGroup.append('g')
            .attr('class', 'batches');

        reduceGroup.append('g')
            .attr('class', 'ops');

        var controllsEnter = this.svg
            .selectAll(".controlls")
            .data([null]).enter()
            .append("g")
            .attr('class', 'controlls')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.updateGroupNames(controllsEnter, gapsFinder.totalWidth);
        this.updateXAxis(controllsEnter, gapsFinder.totalWidth);
        this.updateYMapAxis(controllsEnter);
        this.updateYReduceAxis(controllsEnter);
        this.updateMapBatchesRanges();
        this.updateReduceBatchesRanges();
        this.updateMapOperations();
        this.updateReduceOperations();
        this.updateGaps(gapsFinder.gapsPositions);
    }

    private updateGroupNames(controllsEnter: D3.Selection, totalWidth: number) {
        var self = this;
        var mapHeight = self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2);
        var reduceHeight = self.selectedReduceIndexes().length * (self.yBarHeight + self.yBarMargin * 2);
        self.svg.select('.map_text')
            .transition()
            .attr('x', -mapHeight / 2)
            .style('opacity', mapHeight > 0 ? 1 : 0);
            
        self.svg.select('.reduce_text')
            .transition()
            .attr('x', -self.reduceGroupOffset - reduceHeight / 2)
            .style('opacity', reduceHeight > 0 ? 1 : 0);

        var textsEnter = controllsEnter.append('g')
            .attr('class', 'left_texts');

        textsEnter.append('text')
            .attr('class', 'map_text')
            .attr("transform", "rotate(-90)")
            .attr("dy", ".71em")
            .attr('x', -mapHeight / 2)
            .attr('y',  -self.margin.left)
            .style("text-anchor", "middle")
            .text('Map')
            .style('opacity', mapHeight > 0 ? 1 : 0);

        textsEnter.append('text')
            .attr('class', 'reduce_text')
            .attr("transform", "rotate(-90)")
            .attr("dy", ".71em")
            .attr('x', -self.reduceGroupOffset - reduceHeight / 2)
            .attr('y', -self.margin.left)
            .style("text-anchor", "middle")
            .text('Reduce')
            .style('opacity', reduceHeight > 0 ? 1 : 0);

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

    private updateYMapAxis(controllsEnter) {
        var self = this;
        controllsEnter.append('g')
            .attr('class', 'y axis map')
            .attr("transform", "translate(0,0)");

        var indexCount = self.selectedMapIndexNames().length;

        self.yMapScale = d3.scale.ordinal()
            .domain(self.selectedMapIndexNames())
            .rangeBands([0, indexCount * (self.yBarHeight + self.yBarMargin * 2)]);

        self.yMapAxis = d3.svg.axis()
            .scale(self.yMapScale)
            .orient("left");

        self.svg.select(".y.axis.map")
            .transition()
            .call(self.yMapAxis);
    }

    private updateYReduceAxis(controllsEnter) {
        var self = this;

        controllsEnter.append('g')
            .attr('class', 'y axis reduce')
            .attr("transform", "translate(0,0)");

        var indexCount = self.selectedReduceIndexes().length;

        self.yReduceScale = d3.scale.ordinal()
            .domain(self.selectedReduceIndexes())
            .rangeBands([self.reduceGroupOffset, self.reduceGroupOffset + indexCount * (self.yBarHeight + self.yBarMargin * 2)]);

        self.yReduceAxis = d3.svg.axis()
            .scale(self.yReduceScale)
            .orient("left");

        self.svg.select(".y.axis.reduce")
            .transition()
            .call(self.yReduceAxis);
    }

    // uses optimalization which assumes that both start and start + size falls into continuous region
    private xScaleExtent(size: number) {
        var self = this;
        return size / 1000 * self.pixelsPerSecond();
    }

    private updateMapBatchesRanges() {
        var self = this;
        var batches = self.svg.select(".map_group").select(".batches")
            .selectAll(".batchRange")
            .data(self.mapJsonData, d => d.StartedAt);

        batches.exit().remove();

        batches.select('rect') 
            .transition()
                .attr('x', (d: indexingBatchInfoDto) => self.xScale(d.StartedAtDate))
                .attr('y', (d: indexingBatchInfoDto) => d3.min(d.PerfStats, v => self.yMapScale(v.indexName)))
                .attr('width', (d: indexingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs))
                .attr('height', (d: indexingBatchInfoDto) => {
                    var extent = d3.extent(d.PerfStats, v => self.yMapScale(v.indexName));
                    return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
                });

        batches.enter()
            .append('g')
                .attr('class', 'batchRange')
                .on('click', function (d) { return self.mapBatchInfoClicked(d, this); })
            .append('rect')
                .attr('x', (d: indexingBatchInfoDto) => self.xScale(d.StartedAtDate))
                .attr('y', (d: indexingBatchInfoDto) => d3.min(d.PerfStats, v => self.yMapScale(v.indexName)))
                .attr('width', 0)
                .attr('height', (d: indexingBatchInfoDto) => {
                    var extent = d3.extent(d.PerfStats, v => self.yMapScale(v.indexName));
                    return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
                })
                .style("fill", d => self.color(d.StartedAt))
             .transition()
                .attr('width', (d: indexingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));
    }

    private updateReduceBatchesRanges() {
        var self = this;
        var batches = self.svg.select('.reduce_group').select(".batches")
            .selectAll(".batchRange")
            .data(self.reduceJsonData, d => d.StartedAt);

        batches.exit().remove();

        batches.select('rect')
            .transition()
            .attr('x', (d: reducingBatchInfoDto) => self.xScale(d.StartedAtDate))
            .attr('y', (d: reducingBatchInfoDto) => d3.min(d.PerfStats, v => self.yReduceScale(v.indexName)))
            .attr('width', (d: reducingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs))
            .attr('height', (d: reducingBatchInfoDto) => {
                var extent = d3.extent(d.PerfStats, v => self.yReduceScale(v.indexName));
                return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
            });

        batches.enter()
            .append('g')
            .attr('class', 'batchRange')
            .on('click', function (d) { return self.reduceBatchInfoClicked(d, this); })
            .append('rect')
            .attr('x', (d: reducingBatchInfoDto) => self.xScale(d.StartedAtDate))
            .attr('y', (d: reducingBatchInfoDto) => d3.min(d.PerfStats, v => self.yReduceScale(v.indexName)))
            .attr('width', 0)
            .attr('height', (d: reducingBatchInfoDto) => {
                var extent = d3.extent(d.PerfStats, v => self.yReduceScale(v.indexName));
                return extent[1] - extent[0] + self.yBarHeight + self.yBarMargin * 2;
            })
            .style("fill", d => self.color(d.StartedAt))
            .transition()
            .attr('width', (d: reducingBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));
    }

    private updateMapOperations() {
        var self = this;
        var batches = self.svg.select('.map_group').select(".ops").selectAll(".opGroup")
            .data(self.mapJsonData, d => d.StartedAt);

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
                (d: indexNameAndMapPerformanceStats) =>
                    "translate(" + self.xScale(self.isoFormat.parse(d.stats.Started)) + "," + self.yMapScale(d.indexName) + ")");

        
        opTransition.select('.main_bar')
            .attr('width', (d: indexNameAndMapPerformanceStats) => self.xScaleExtent(d.stats.DurationMilliseconds));
        
        opTransition.selectAll('.sto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 
        
        opTransition.selectAll('.mto_items')
            .attr("transform", (d: parallelPefromanceStatsDto) => "translate(" + self.xScaleExtent(d.CacheCumulativeSum) + ",0)")
        
        opTransition.selectAll('.mto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        var enteringOps = op.enter()
            .append('g')
            .attr('class', 'op')
            .on('click', function (d) { return self.indexStatClicked(d, this); })
            .attr("transform",
            (d: indexNameAndMapPerformanceStats) =>
                "translate(" + self.xScale(self.isoFormat.parse(d.stats.Started)) + "," + self.yMapScale(d.indexName) + ")");

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
            .attr('width', (d: indexNameAndMapPerformanceStats) => self.xScaleExtent(d.stats.DurationMilliseconds));

        enteringOps.selectAll('.sto_item')
            .data((d: indexNameAndMapPerformanceStats) => d.stats.Operations.filter(o => o.CacheIsSingleThread))
            .enter()
            .append('rect')
            .attr('class', (d: performanceStatsDto) => 'sto_item ' + d.Name)
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .transition()
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        var mtoItems = enteringOps.selectAll('.mto_items')
            .data((d: indexNameAndMapPerformanceStats) => d.stats.Operations.filter(o => !o.CacheIsSingleThread))
            .enter()
            .append('g')
            .attr('class', 'mto_items')
            .attr("transform",
            (d: parallelPefromanceStatsDto) =>
                "translate(" + self.xScaleExtent(d.CacheCumulativeSum) + ",0)");

        var mtoThreadItems = mtoItems.selectAll('.mto_thread_item')
            .data(d => d.BatchedOperations)
            .enter()
            .append('g')
            .attr('class', 'mto_thread_item')
            .attr("transform",
            (d: parallelBatchStatsDto, i) =>
                "translate(0," + (i *self.yBarHeight / d.Parent.NumberOfThreads + self.yBarMargin) + ")");

        mtoThreadItems.selectAll('.mto_item')
            .data(d => d.Operations)
            .enter()
            .append('rect')
            .attr('class', (d: performanceStatsDto) => 'mto_item ' + d.Name)
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('y', 0)
            .attr('width', 0)
            .attr('height', (d:performanceStatsDto) => self.yBarHeight / d.ParallelParent.Parent.NumberOfThreads)
            .transition()
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 
    }

    private updateReduceOperations() {
        var self = this;
        var batches = self.svg.select('.reduce_group').select(".ops").selectAll(".opGroup")
            .data(self.reduceJsonData, d => d.StartedAt);

        batches.exit().remove();

        var enteringOpsGroups = batches
            .enter()
            .append('g')
            .attr('class', 'opGroup');

        var op = batches.selectAll('.op_g')
            .data((d: reducingBatchInfoDto) => d.PerfStats, d => d.indexName);

        op.exit().remove();

        var opTransition = 
            op
                .selectAll('.op')
                .transition()
                .attr("transform",
                (d: reduceLevelPeformanceStatsDto) =>
                    "translate(" + self.xScale(self.isoFormat.parse(d.Started)) + "," + self.yReduceScale(d.parent.indexName) + ")");

        opTransition.select('.main_bar')
            .attr('width', (d: reduceLevelPeformanceStatsDto) => self.xScaleExtent(d.DurationMs));
        
        for (var i = 0; i < metrics.reduce_bar_names.length; i++) {
            var barName = metrics.reduce_bar_names[i];
            opTransition.select('.' + barName)
                .attr('x', (d: reduceLevelPeformanceStatsDtoWithCache) => self.xScaleExtent(d.cumulativeSums[i]))
                .attr('width', (d: reduceLevelPeformanceStatsDtoWithCache) => self.xScaleExtent(d.widths[i]));
        }

        var enteringOps = op.enter()
            .append('g')
            .attr('class', 'op_g')
            .selectAll('op')
            .data(d => d.stats.LevelStats)
            .enter()
            .append('g')
            .attr('class', 'op')
            .on('click', function (d) { return self.reduceStatClicked(d, this); })
            .attr("transform",
            (d: reduceLevelPeformanceStatsDto) =>
                "translate(" + self.xScale(self.isoFormat.parse(d.Started)) + "," + self.yReduceScale(d.parent.indexName) + ")");

        enteringOps
            .filter((d, i) => i > 0)
            .append('line')
            .attr('class', 'reduce_split_line')
            .attr('x1', 0)
            .attr('x2', 0)
            .attr('y1', 0)
            .attr('y2', self.yBarHeight + self.yBarMargin * 2);

        enteringOps.append('rect')
            .attr('class', 'main_bar')
            .attr('x', 0)
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .style('fill', (d:reduceLevelPeformanceStatsDto) => self.color(d.parent.parent.StartedAt))
            .transition()
                .attr('width', (d: reduceLevelPeformanceStatsDto) => self.xScaleExtent(d.DurationMs));

        for (var i = 0; i < metrics.reduce_bar_names.length; i++) {
            var barName = metrics.reduce_bar_names[i];
            var barColor = metrics.reduce_bar_colors[i];
            enteringOps.append('rect')
                .attr('class', barName)
                .attr('x', (d: reduceLevelPeformanceStatsDtoWithCache) => self.xScaleExtent(d.cumulativeSums[i]))
                .attr('y', self.yBarMargin + 3)
                .attr('width', 0)
                .attr('height', self.yBarHeight - 6)
                .style('fill', barColor)
                .transition()
                .attr('width', (d: reduceLevelPeformanceStatsDtoWithCache) => self.xScaleExtent(d.widths[i]));
        }
    }

    private updateGaps(gapsPositions: timeGap[]) {
        var self = this;
        var gaps = self.svg.select('.gaps').selectAll('.gap')
            .data(gapsPositions);

        gaps.exit().remove();

        var patternWidth = 20;
        var gapHeight = (self.selectedMapIndexNames().length + self.selectedReduceIndexes().length) * (self.yBarHeight + self.yBarMargin * 2) + self.margin.between;

        gaps.select('text')
            .transition()
            .attr("y", d => d.position)
            .attr('x', - gapHeight / 2);

        gaps.select('rect')
            .transition()
                .attr('x', d => d.position - patternWidth / 2)
                .attr('height', gapHeight);

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
            .attr('height', gapHeight);

        enteringGaps.append('text')
            .attr("transform", "rotate(-90)")
            .attr("y", d => d.position)
            .attr('x', -gapHeight / 2)
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

    private findMaxThreadCountInMap(data: indexNameAndMapPerformanceStats) {
        var parallelOps = <parallelPefromanceStatsDto[]>data.stats.Operations.filter(x => "NumberOfThreads" in x);
        return d3.max(parallelOps.map(p => p.NumberOfThreads));
    }

    private indexStatClicked(data: indexNameAndMapPerformanceStats, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'index-stat-template\' }"></div>'; 
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width + 10, offset.top], html, 's', 0, null, "selectable-tooltip");
        data.CacheThreadCount = self.findMaxThreadCountInMap(data);
        self.fillTooltip(data, metrics.map_bar_colors);
    }

    private reduceStatClicked(data: indexNameAndReducingPerformanceStats, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'reduce-stat-template\' }"></div>';
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width + 10, offset.top], html, 's', 0, null, "selectable-tooltip");
        self.fillTooltip(data, metrics.reduce_bar_colors);
    }

    private mapBatchInfoClicked(data: indexingBatchInfoDto, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'map-batch-info-template\' }"></div>'; 
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width + 10, offset.top], html, 's', 0, null, "selectable-tooltip");
        self.fillTooltip(data, metrics.map_bar_colors);
    }

    private reduceBatchInfoClicked(data: indexingBatchInfoDto, element: Element) {
        var self = this;
        nv.tooltip.cleanup();
        var offset = $(element).offset();
        var containerOffset = $("#metricsContainer").offset();
        var html = '<div data-bind="template: { name : \'reduce-batch-info-template\' }"></div>';
        nv.tooltip.show([offset.left + element.getBoundingClientRect().width + 10, offset.top], html, 's', 0, null, "selectable-tooltip");
        self.fillTooltip(data, metrics.reduce_bar_colors);
    }

    private fillTooltip(model: any, colors: string[]) {
        var node = $(".nvtooltip")[0];
        ko.applyBindings({ data: model, colors: colors, tooltipClose:  nv.tooltip.cleanup }, node);
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

    findIndexNames(jsonData: any) {
        var statsInline = d3.merge(jsonData.map(d => d.PerfStats));
        var byKey = d3
            .nest()
            .key(d => d.indexName)
            .sortKeys(d3.ascending)
            .rollup(l => l.length)
            .entries(statsInline);
        return byKey.map(d => d.key);
    }

    private zoomingIn = false;
    private zoomingOut = false;

    startZoomIn() {
        this.zoomingIn = true;
        this.zoomIn();
    }

    startZoomOut() {
        this.zoomingOut = true;
        this.zoomOut();
    }

    stopZoomIn() {
        this.zoomingIn = false;
    }

    stopZoomOut() {
        this.zoomingOut = false;
    }

    zoomIn() {
        if (this.zoomingIn) {
            this.pixelsPerSecond(this.pixelsPerSecond() + 20);
            setTimeout(() => this.zoomIn(), 100);
        }
    }

    zoomOut() {
        if (this.zoomingOut) {
            if (this.pixelsPerSecond() > 20) {
                this.pixelsPerSecond(this.pixelsPerSecond() - 20);
            }
            setTimeout(() => this.zoomOut(), 100);
        }
    }
}

export = metrics; 
