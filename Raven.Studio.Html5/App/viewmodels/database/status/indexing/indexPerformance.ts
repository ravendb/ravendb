/// <reference path="../../../../../Scripts/typings/bootstrap.multiselect/bootstrap.multiselect.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import changesContext = require("common/changesContext");
import getIndexingBatchStatsCommand = require("commands/database/debug/getIndexingBatchStatsCommand");
import getReducingBatchStatsCommand = require("commands/database/debug/getReducingBatchStatsCommand");
import getDeletionBatchStatsCommand = require("commands/database/debug/getDeletionBatchStatsCommand");
import getDatabaseStatsCommand = require("commands/resources/getDatabaseStatsCommand");
import getFilteredOutIndexStatsCommand = require("commands/database/debug/getFilteredOutIndexStatsCommand");
import d3 = require('d3/d3');
import nv = require('nvd3');
import app = require("durandal/app");
import changeSubscription = require('common/changeSubscription');
import mapBatchInfo = require("viewmodels/database/status/indexing/perfDialogs/mapBatchInfo");
import reduceBatchInfo = require("viewmodels/database/status/indexing/perfDialogs/reduceBatchInfo");
import deletionBatchInfo = require("viewmodels/database/status/indexing/perfDialogs/deletionBatchInfo");
import mapStatsInfo = require("viewmodels/database/status/indexing/perfDialogs/mapStatsInfo");
import reduceStatsInfo = require("viewmodels/database/status/indexing/perfDialogs/reduceStatsInfo");
import prefetchInfo = require("viewmodels/database/status/indexing/perfDialogs/prefetchInfo");
import filteredOutIndexInfo = require("viewmodels/database/status/indexing/perfDialogs/filteredOutIndexInfo");
import awesomeMultiselect = require("common/awesomeMultiselect");

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
        if (this.domain.length === 0) {
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
                if (newRange.end.getTime() > e.getTime()) {
                    e = newRange.end;
                }
            }
            this.pushRegion(new dateRange(s, e));
        }
    }
}
class metrics extends viewModelBase { 

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

    edgeDate: Date;

    mapGroupOffset = 0;
    reduceGroupOffset = 0;
    deletionGroupOffset = 0;
    
    mapJsonData: indexingBatchInfoDto[] = [];
    rawMapJsonData: indexingBatchInfoDto[] = [];

    reduceJsonData: reducingBatchInfoDto[] = [];
    rawReduceJsonData: reducingBatchInfoDto[] = [];

    deletionJsonData: deletionBatchInfoDto[] = [];
    rawDeletionJsonData: deletionBatchInfoDto[] = [];

    prefetchJsonData: futureBatchStatsDto[] = [];
    rawPrefetchJsonData: futureBatchStatsDto[] = [];

    filteredOutIndexesJsonData: filteredOutIndexStatDto[] = [];
    rawFilteredOutIndexesJsonData: filteredOutIndexStatDto[] = [];
    
    mapAllIndexNames = ko.observableArray<string>();
    reduceAllIndexNames = ko.observableArray<string>();
    deletionAllIndexNames = ko.observableArray<string>();

    allIndexNames = ko.computed(() => d3.set(this.mapAllIndexNames().concat(this.reduceAllIndexNames(), this.deletionAllIndexNames())).values());
    selectedIndexNames = ko.observableArray<string>();
    selectedMapIndexNames = ko.computed(() => this.selectedIndexNames().filter(x => this.mapAllIndexNames().contains(x)));
    selectedReduceIndexes = ko.computed(() => this.selectedIndexNames().filter(x => this.reduceAllIndexNames().contains(x)));
    selectedDeletionIndexes = ko.computed(() => this.selectedIndexNames().filter(x => this.deletionAllIndexNames().contains(x)));

    static batchesColors = ['#ececec', '#c2c2c2', '#959595'];

    color = d3.scale.ordinal().range(metrics.batchesColors);
    margin = { top: 40, right: 20, bottom: 40, left: 200, between: 10 };

    prefetchingLineHeight = this.yBarHeight + this.yBarMargin * 2 + this.margin.between;

    private pixelsPerSecond = ko.observable<number>(100);

    width: number;

    isoFormat = d3.time.format.iso;
    xTickFormat = d3.time.format("%H:%M:%S");	

    xScale: D3.Scale.TimeScale;
    yMapScale: D3.Scale.OrdinalScale;
    yReduceScale: D3.Scale.OrdinalScale;
    yDeletionScale: D3.Scale.OrdinalScale;
    yPrefetchScale: D3.Scale.OrdinalScale;

    xAxis: D3.Svg.Axis;
    yMapAxis: D3.Svg.Axis;
    yReduceAxis: D3.Svg.Axis;
    yDeletionAxis: D3.Svg.Axis;
    yPrefetchAxis: D3.Svg.Axis;

    svg: D3.Selection;

    lastIndexingId: number = 0;
    lastReducingId: number = 0;
    lastDeletionId: number = 0;

    private refreshGraphObservable = ko.observable<number>();
    private refreshSubscription: KnockoutSubscription;

    constructor() {
        super();
        this.pixelsPerSecond.throttle(100).subscribe(() => {
            nv.tooltip.cleanup();
            this.redrawGraph();
        });
    }

    fetchMapJsonData() {
        return new getIndexingBatchStatsCommand(this.activeDatabase(), this.lastIndexingId).execute();
    }

    fetchReduceJsonData() {
        return new getReducingBatchStatsCommand(this.activeDatabase(), this.lastReducingId).execute();
    }

    fetchDeletionJsonData() {
        return new getDeletionBatchStatsCommand(this.activeDatabase(), this.lastDeletionId).execute();
    }

    fetchPrefetcherJsonData() {
        var promise = $.Deferred();
        new getDatabaseStatsCommand(this.activeDatabase())
            .execute()
            .done((result) => promise.resolve(result.Prefetches));

        return promise;
    }

    fetchFilteredOutJsonData() {
        return new getFilteredOutIndexStatsCommand(this.activeDatabase())
            .execute();
    }

    activate(args) {
        super.activate(args);

        $(document).bind("fullscreenchange", function () {
            if ($(document).fullScreen()) {
                $("#fullScreenButton i").removeClass("fa-expand").addClass("fa-compress");
            } else {
                $("#fullScreenButton i").removeClass("fa-compress").addClass("fa-expand");
            }
        });
    }

    attached() {
        super.attached();
        this.createKeyboardShortcut("esc", nv.tooltip.cleanup, "body");
        this.updateHelpLink('QCVU81');
        $("#metricsContainer").resize().on('DynamicHeightSet', () => this.onWindowHeightChanged());
        $("#metricsContainer").scroll(() => this.graphScrolled());
        
        this.refreshSubscription = this.refreshGraphObservable.throttle(5000).subscribe(() => this.refresh());
      
        awesomeMultiselect.build($("#visibleIndexesSelector"));
        this.svg = d3.select("#indexPerformanceGraph");
    }

    compositionComplete() {
        super.compositionComplete();
        this.selectedIndexNames.subscribe((v) => {
            this.filterJsonData();
            nv.tooltip.cleanup();
            this.redrawGraph();
        });
        this.refresh();
    }

    createNotifications(): Array<changeSubscription> {
        return [changesContext.currentResourceChangesApi().watchAllIndexes(e => this.processIndexEvent(e))];
    }

    processIndexEvent(e: indexChangeNotificationDto) {
        if (e.Type === "MapCompleted" || e.Type === "ReduceCompleted" || e.Type === "RemoveFromIndex") {
            this.refreshGraphObservable(new Date().getTime());
        }
    }

    filterJsonData() {
        this.mapJsonData = [];
        this.reduceJsonData = [];
        this.deletionJsonData = [];
        this.prefetchJsonData = [];
        this.filteredOutIndexesJsonData = [];
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
        this.deletionJsonData = this.rawDeletionJsonData.filter(x => selectedIndexes.contains(x.IndexName));
        this.rawPrefetchJsonData.forEach(rawData => {
            this.prefetchJsonData.push(rawData);
        });
        this.rawFilteredOutIndexesJsonData.forEach(rawData => {
            if (selectedIndexes.contains(rawData.IndexName)) {
                this.filteredOutIndexesJsonData.push(rawData);
            }
        });
    }

    showHideNoData(show: boolean) {
        if (show) {
            var metricsContainer = $("#metricsContainer");
            this.svg.select('.no_data').remove();
            this.svg.append('text')
                .attr('class', 'no_data')
                .text('No data available')
                .attr('fill', 'black')
                .attr('text-anchor', 'middle')
                .attr('x', metricsContainer.width() / 2)
                .attr('y', 50);
            this.svg.select('.controlls').style('display', 'none');
        } else {
            this.svg.select('.no_data').remove();
            this.svg.select('.controlls').style('display', null);
        }
    }

    refresh() {
        var mapTask = this.fetchMapJsonData();
        var reduceTask = this.fetchReduceJsonData();
        var prefetchTask = this.fetchPrefetcherJsonData();
        var filterTask = this.fetchFilteredOutJsonData();
        var deletionTask = this.fetchDeletionJsonData();

        $.when(mapTask, reduceTask, prefetchTask, filterTask, deletionTask)
            .done((mapResult: any[], reduceResult: any[], prefetches: futureBatchStatsDto[], filteredOutIndexes: filteredOutIndexStatDto[], deletionResult: any[]) => {
                var oldAllIndexes = this.allIndexNames();

                if (mapResult.length > 0) {
                    this.lastIndexingId = d3.max(mapResult, x => x.Id);
                }
                if (reduceResult.length > 0) {
                    this.lastReducingId = d3.max(reduceResult, x => x.Id);
                }
                if (deletionResult.length > 0) {
                    this.lastDeletionId = d3.max(deletionResult, x => x.Id);
                }

                if (oldAllIndexes.length === 0) {
                    if (mapResult.length > 0) {
                        this.edgeDate = d3.min(mapResult, r => r.StartedAtDate);
                    } else if (reduceResult.length > 0) {
                        this.edgeDate = d3.min(reduceResult, r => r.StartedAtDate);
                    }
                }
                mapResult = mapResult.filter(r => r.StartedAtDate >= this.edgeDate);
                reduceResult = reduceResult.filter(r => r.StartedAtDate >= this.edgeDate);
                filteredOutIndexes = filteredOutIndexes.filter(r => r.TimestampParsed >= this.edgeDate);

                this.rawFilteredOutIndexesJsonData = this.mergeFilteredOutIndexesJsonData(this.rawFilteredOutIndexesJsonData, filteredOutIndexes);
                this.rawPrefetchJsonData = this.mergePrefetchJsonData(this.rawPrefetchJsonData, prefetches);

                this.rawMapJsonData = this.mergeMapJsonData(this.rawMapJsonData, mapResult);
                var mapIndexes = this.findIndexNames(this.rawMapJsonData);
                var filteredOutResourcesIndexNames = this.rawFilteredOutIndexesJsonData.map(x => x.IndexName);

                // merge filtered out resources with map index names as we display them in map section
                mapIndexes = d3.set(mapIndexes.concat(filteredOutResourcesIndexNames)).values();

                var oldMapAllIndexes = this.mapAllIndexNames();
                var newMapIndexes = mapIndexes.filter(i => !oldMapAllIndexes.contains(i));
                this.mapAllIndexNames.pushAll(newMapIndexes);

                this.rawReduceJsonData = this.mergeReduceJsonData(this.rawReduceJsonData, reduceResult);
                var reduceIndexes = this.findIndexNames(this.rawReduceJsonData);
                var oldReduceAllIndexes = this.reduceAllIndexNames();
                var newReduceIndexes = reduceIndexes.filter(i => !oldReduceAllIndexes.contains(i));
                this.reduceAllIndexNames.pushAll(newReduceIndexes);

                this.rawDeletionJsonData = this.mergeDeletionJsonData(this.rawDeletionJsonData, deletionResult);
                var deletionIndexes = d3.set(this.rawDeletionJsonData.map(x => x.IndexName)).values();
                var oldDeletionAllIndexes = this.deletionAllIndexNames();
                var newDeletionIndexes = deletionIndexes.filter(i => !oldDeletionAllIndexes.contains(i));
                this.deletionAllIndexNames.pushAll(newDeletionIndexes);

                var newIndexes = d3.set(mapIndexes.concat(reduceIndexes, deletionIndexes)).values().filter(i => !oldAllIndexes.contains(i));

                // this will filterJsonData and redrawGraph
                this.selectedIndexNames(this.selectedIndexNames().concat(newIndexes));
                // refresh multiselect widget:
                awesomeMultiselect.rebuild($("#visibleIndexesSelector"));
            });
    }

    private createFilteredOutCacheKey(input: filteredOutIndexStatDto) {
        return input.Timestamp + "$" + input.IndexName;
    }

    private mergeFilteredOutIndexesJsonData(currentData: filteredOutIndexStatDto[], incomingData: filteredOutIndexStatDto[]) {
        // create lookup map to avoid O(n^2)
        var dateLookup = d3.map();

        currentData.forEach((d, i) => {
            dateLookup.set(this.createFilteredOutCacheKey(d), i);
        });

        incomingData.forEach(d => {
            var cacheKey = this.createFilteredOutCacheKey(d);
            if (dateLookup.has(cacheKey)) {
                var index = dateLookup.get(cacheKey);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });

        return currentData;
    }

    private mergePrefetchJsonData(currentData: futureBatchStatsDto[], incomingData: futureBatchStatsDto[]) {
        // create lookup map to avoid O(n^2)
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.Timestamp, i);
        });

        incomingData.forEach(d => {

            // we have to filter out future batches that are not completed
            if (!d.Duration) {
                return;
            }

            this.computePrefetchCache(d);
            if (dateLookup.has(d.Timestamp)) {
                var index = dateLookup.get(d.Timestamp);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });

        return currentData;
    }

    private mergeMapJsonData(currentData: indexingBatchInfoDto[], incomingData: indexingBatchInfoDto[]) {
        // create lookup map to avoid O(n^2) 
        var self = this;
        var dateLookup = d3.map();
        currentData.forEach((d, i) => {
            dateLookup.set(d.StartedAt, i);
        });

        incomingData.forEach(d => {

            d.PerfStats.forEach(self.computeMapCache.bind(self));

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
            d.PerfStats.forEach(s => s.stats.LevelStats.forEach(self.computeReduceCache.bind(self)));

            if (dateLookup.has(d.StartedAt)) {
                var index = dateLookup.get(d.StartedAt);
                currentData[index] = d;
            } else {
                currentData.push(d);
            }
        });
        return currentData;
    }

    private mergeDeletionJsonData(currentData: deletionBatchInfoDto[], incomingData: deletionBatchInfoDto[]) {
        var maxIdInCurrentData = currentData.length === 0 ? -1 : currentData[currentData.length - 1].Id;
        var newItems = incomingData.filter(x => x.Id > maxIdInCurrentData);

        newItems.forEach(item => this.computeDeletionCache(item));

        currentData.pushAll(newItems);
        return currentData;
    }

    computeMapCache(input: indexNameAndMapPerformanceStats) {
        var currentOffset = 0;
        var self = this;

        var timings = input.stats.Operations.map(o => o.DurationMs);

        for (var i = 0; i < timings.length; i++) {
            var currentWidth = Math.max(timings[i], 0);

            var op = input.stats.Operations[i];
            op.CacheWidth = currentWidth;
            op.CacheCumulativeSum = currentOffset;
            op.CacheIsSingleThread = "Name" in op;

            if (!op.CacheIsSingleThread) {
                var mtGroup = <parallelPefromanceStatsDto>op;
                self.computeParallelOpsCache(mtGroup);
            }

            currentOffset += currentWidth;
        }
    }

    computeReduceCache(input: reduceLevelPeformanceStatsDto) {
        var currentOffset = 0;
        var self = this;

        var timings = input.Operations.map(o => o.DurationMs);
        for (var i = 0; i < timings.length; i++) {
            var currentWidth = Math.max(timings[i], 0);
            var op = input.Operations[i];
            op.CacheWidth = currentWidth;
            op.CacheCumulativeSum = currentOffset;
            op.CacheIsSingleThread = "Name" in op;

            if (!op.CacheIsSingleThread) {
                var mtGroup = <parallelPefromanceStatsDto>op;
                self.computeParallelOpsCache(mtGroup);
            }

            currentOffset += currentWidth;
        }
    }

    computeDeletionCache(input: deletionBatchInfoDto) {
        var currentOffset = 0;

        var timings = input.PerformanceStats.map(o => o.DurationMs);
        for (var i = 0; i < timings.length; i++) {
            var currentWidth = Math.max(timings[i], 0);
            var stat = input.PerformanceStats[i];
            stat.CacheWidth = currentWidth;
            stat.CacheCumulativeSum = currentOffset;

            currentOffset += currentWidth;
        }
    }

    computePrefetchCache(input: futureBatchStatsDto) {
        var inputAny = <any>input;

        inputAny.CacheTimestamp = this.isoFormat.parse(input.Timestamp);
        inputAny.DurationMs = this.parseTimespan(input.Duration);
    }

    computeParallelOpsCache(mtGroup: parallelPefromanceStatsDto) {
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

    graphScrolled() {
        var leftScroll = $("#metricsContainer").scrollLeft();
        this.svg.select('.y.axis.map')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('.y.axis.reduce')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('.y.axis.prefetch')
            .attr("transform", "translate(" + leftScroll + ",0)");

        this.svg.select('.y.axis.deletion')
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

        var deletionDateRange = this.deletionJsonData.map(
            j => new dateRange(j.StartedAtDate, new Date(j.StartedAtDate.getTime() + j.TotalDurationMs)));

        var filteredOutDateRange = this.filteredOutIndexesJsonData.map(
            j => new dateRange(j.TimestampParsed, new Date(j.TimestampParsed.getTime() + 10)));

        var mergedDateRange = mapDateRange.concat(reduceDateRange, filteredOutDateRange, deletionDateRange).sort((a, b) => a.start.getTime() - b.start.getTime());

        var gapsFinder = new gapFinder(mergedDateRange, self.pixelsPerSecond(), metrics.minGapTime);

        self.xScale = gapsFinder.constructScale();

        var totalWidthWithMargins = gapsFinder.totalWidth
            + self.margin.left
            + self.margin.right
            + 10; // add few more extra pixels

        totalWidthWithMargins = Math.max(totalWidthWithMargins, this.width);
        
        var totalHeight =
            self.prefetchingLineHeight
            + self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.between
            + self.selectedReduceIndexes().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.between
            + self.selectedDeletionIndexes().length * (self.yBarHeight + self.yBarMargin * 2)
            + self.margin.top
            + self.margin.bottom;
        this.svg = d3.select("#indexPerformanceGraph");

        (this.svg.select('defs').node() !== null ? <any>this.svg.transition() : <any>this.svg)
            .attr("width", totalWidthWithMargins)
            .attr("height", totalHeight)
            .style('height', totalHeight + 'px')
            .style('width', totalWidthWithMargins + 'px')
            .attr("viewBox", "0 0 " + totalWidthWithMargins + " " + totalHeight);

        self.mapGroupOffset = self.prefetchingLineHeight;
        self.reduceGroupOffset = self.mapGroupOffset + self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2) + self.margin.between;
        self.deletionGroupOffset = self.reduceGroupOffset + self.selectedReduceIndexes().length * (self.yBarHeight + self.yBarMargin * 2) + self.margin.between;

        this.svg.selectAll('.map_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.svg.selectAll('.reduce_group')
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        this.svg.selectAll('.deletion_group')
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
            .attr('stroke', '#333')
            .attr('fill', 'transparent')
            .attr('stroke-width', 0.25)
            .attr('stroke-linecap', 'square')
            .attr('stroke-linejoin', 'miter');

        defs.append('pattern')
            .attr('id', 'lines')
            .attr('x', 0)
            .attr('y', 0)
            .attr('width', 5)
            .attr('height', 5)
            .attr('patternUnits', 'userSpaceOnUse')
            .append('path')
            .attr('d', 'M 0 5 L 5 0')
            .attr('stroke', '#333')
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

        var deletionGroup = svgEnter.append('g')
            .attr('class', 'deletion_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        deletionGroup.append('g')
            .attr('class', 'batches');

        deletionGroup.append('g')
            .attr('class', 'ops');

        svgEnter.append('g')
            .attr('class', 'prefetch_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

        svgEnter.append('g')
            .attr('class', 'filtered_out_group')
            .attr('clip-path', "url(#dataClip)")
            .attr("transform", "translate(" + self.margin.left + "," + self.margin.top + ")");

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
        this.updateYDeletionAxis(controllsEnter);
        this.updateYPrefetchAxis(controllsEnter);
        this.updateMapBatchesRanges();
        this.updateReduceBatchesRanges();
        this.updateDeletionBatchesRanges();
        this.updateMapOperations();
        this.updateReduceOperations();
        this.updateDeletionOperations();
        this.updatePrefetchOperations();
        this.updateFilteredOutIndexesOperations();
        this.updateGaps(gapsFinder.gapsPositions);
        this.showHideNoData(this.rawMapJsonData.length === 0 && this.rawReduceJsonData.length === 0 && this.rawDeletionJsonData.length === 0);
    }

    private updateGroupNames(controllsEnter: D3.Selection, totalWidth: number) {
        var self = this;
        var mapHeight = self.selectedMapIndexNames().length * (self.yBarHeight + self.yBarMargin * 2);
        var reduceHeight = self.selectedReduceIndexes().length * (self.yBarHeight + self.yBarMargin * 2);
        var deletionHeight = self.selectedDeletionIndexes().length * (self.yBarHeight + self.yBarMargin * 2);

        self.svg.select('.map_text')
            .transition()
            .attr('x', -self.mapGroupOffset - mapHeight / 2)
            .style('opacity', mapHeight > 0 ? 1 : 0);
            
        self.svg.select('.reduce_text')
            .transition()
            .attr('x', -self.reduceGroupOffset - reduceHeight / 2)
            .style('opacity', reduceHeight > 0 ? 1 : 0);

        self.svg.select('.deletion_text')
            .transition()
            .attr('x', -self.deletionGroupOffset - deletionHeight / 2)
            .style('opacity', deletionHeight > 0 ? 1 : 0);

        var textsEnter = controllsEnter.append('g')
            .attr('class', 'left_texts');

        textsEnter.append('text')
            .attr('class', 'map_text')
            .attr("transform", "rotate(-90)")
            .attr("dy", ".71em")
            .attr('x', -self.mapGroupOffset - mapHeight / 2)
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

        textsEnter.append('text')
            .attr('class', 'deletion_text')
            .attr("transform", "rotate(-90)")
            .attr("dy", ".71em")
            .attr('x', -self.deletionGroupOffset - deletionHeight / 2)
            .attr('y', -self.margin.left)
            .style("text-anchor", "middle")
            .text('Deletion')
            .style('opacity', deletionHeight > 0 ? 1 : 0);

        textsEnter.append('text')
            .attr('class', 'prefetch_text')
            .attr("transform", "rotate(-90)")
            .attr("dy", ".71em")
            .attr('x', -self.margin.top)
            .attr('y', -self.margin.left)
            .style("text-anchor", "middle")
            .text('Prefetch')
            .style('opacity', self.prefetchJsonData.length > 0 ? 1 : 0); 
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
            .rangeBands([self.mapGroupOffset, self.mapGroupOffset + indexCount * (self.yBarHeight + self.yBarMargin * 2)]);

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

    private updateYDeletionAxis(controllsEnter) {
        var self = this;

        controllsEnter.append('g')
            .attr('class', 'y axis deletion')
            .attr('transform', "translate(0,0)");

        var indexCount = self.selectedDeletionIndexes().length;

        self.yDeletionScale = d3.scale.ordinal()
            .domain(self.selectedDeletionIndexes())
            .rangeBands([self.deletionGroupOffset, self.deletionGroupOffset + indexCount * (self.yBarHeight + self.yBarMargin * 2)]);

        self.yDeletionAxis = d3.svg.axis()
            .scale(self.yDeletionScale)
            .orient("left");

        self.svg.select(".y.axis.deletion")
            .transition()
            .call(self.yDeletionAxis);
    }

    private updateYPrefetchAxis(controllsEnter) {
        var self = this;

        controllsEnter.append('g')
            .attr('class', 'y axis prefetch')
            .attr("transform", "translate(0,0)");

        self.yPrefetchScale = d3.scale.ordinal()
            .domain([ "Prefetching "])
            .rangeBands([0, (self.yBarHeight + self.yBarMargin * 2)]);

        self.yPrefetchAxis = d3.svg.axis()
            .scale(self.yPrefetchScale)
            .orient("left");

        self.svg.select(".y.axis.prefetch")
            .transition()
            .call(self.yPrefetchAxis);
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
                .on('click', self.mapBatchInfoClicked.bind(self))
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
            .on('click', self.reduceBatchInfoClicked.bind(self))
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

    private updateDeletionBatchesRanges() {
        var self = this;
        var batches = self.svg.select('.deletion_group').select(".batches")
            .selectAll(".batchRange")
            .data(self.deletionJsonData, d => d.Id);

        batches.exit().remove();

        batches.select('rect')
            .transition()
            .attr('x', (d: deletionBatchInfoDto) => self.xScale(d.StartedAtDate))
            .attr('y', (d: deletionBatchInfoDto) => self.yDeletionScale(d.IndexName))
            .attr('width', (d: deletionBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs))
            .attr('height', (d: deletionBatchInfoDto) => self.yBarHeight + self.yBarMargin * 2);

        batches.enter()
            .append('g')
            .attr('class', 'batchRange')
            .on('click', self.deletionBatchInfoClicked.bind(self)) 
            .append('rect')
            .attr('x', (d: deletionBatchInfoDto) => self.xScale(d.StartedAtDate))
            .attr('y', (d: deletionBatchInfoDto) => self.yDeletionScale(d.IndexName))
            .attr('width', 0)
            .attr('height', (d: deletionBatchInfoDto) => self.yBarHeight + self.yBarMargin * 2)
            .style("fill", d => self.color(d.StartedAt))
            .transition()
            .attr('width', (d: deletionBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));
    }

    private updateMapOperations() {
        var self = this;
        var batches = self.svg.select('.map_group').select(".ops").selectAll(".opGroup")
            .data(self.mapJsonData, d => d.StartedAt);

        batches.exit().remove();

        batches
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
        
        opTransition.selectAll('.mto_hatch')
            .attr('width', (d: parallelPefromanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        opTransition.selectAll('.sto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 
        
        opTransition.selectAll('.mto_items')
            .attr("transform", (d: parallelPefromanceStatsDto) => "translate(" + self.xScaleExtent(d.CacheCumulativeSum) + ",0)");
        
        opTransition.selectAll('.mto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        var enteringOps = op.enter()
            .append('g')
            .attr('class', 'op')
            .on('click',  self.indexStatClicked.bind(self))
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

        mtoItems
            .append('rect')
            .attr('class', 'mto_hatch')
            .attr('x', 0)
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight) 
            .style('fill', 'url(#lines)')
            .transition()
            .attr('width', (d: parallelPefromanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 
            

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

        batches
            .enter()
            .append('g')
            .attr('class', 'opGroup');

        var op = batches.selectAll('.op_g')
            .data((d: reducingBatchInfoDto) => d.PerfStats, d => d.indexName + "/" + d.stats.ReduceType);

        op.exit().remove();

        var opTransition =
            op
                .selectAll('.op') 
                .transition()
                .attr("transform", 
                (d: reduceLevelPeformanceStatsDto) => "translate(" + self.xScale(self.isoFormat.parse(d.Started)) + "," + self.yReduceScale(d.parent.indexName) + ")");

        opTransition.select('.main_bar')
            .attr('width', (d: reduceLevelPeformanceStatsDto) => self.xScaleExtent(d.DurationMs));

        opTransition.selectAll('.mto_hatch')
            .attr('width', (d: parallelPefromanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        opTransition.selectAll('.sto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        opTransition.selectAll('.mto_items')
            .attr("transform", (d: parallelPefromanceStatsDto) => "translate(" + self.xScaleExtent(d.CacheCumulativeSum) + ",0)");

        opTransition.selectAll('.mto_item')
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        var enteringOps = op.enter()
            .append('g')
            .attr('class', 'op_g')
            .selectAll('op')
            .data(d => d.stats.LevelStats)
            .enter()
            .append('g')
            .attr('class', 'op')
            .on('click', self.reduceStatClicked.bind(self))
            .attr("transform",
            (d: reduceLevelPeformanceStatsDto) =>
                "translate(" + self.xScale(self.isoFormat.parse(d.Started)) + "," + self.yReduceScale(d.parent.indexName) + ")");

        enteringOps
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

        enteringOps.selectAll('.sto_item')
            .data((d: reduceLevelPeformanceStatsDto) => d.Operations.filter(o => o.CacheIsSingleThread))
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
            .data((d: reduceLevelPeformanceStatsDto) => d.Operations.filter(o => !o.CacheIsSingleThread))
            .enter()
            .append('g')
            .attr('class', 'mto_items')
            .attr("transform",
            (d: parallelPefromanceStatsDto) =>
                "translate(" + self.xScaleExtent(d.CacheCumulativeSum) + ",0)");

        mtoItems
            .append('rect')
            .attr('class', 'mto_hatch')
            .attr('x', 0)
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .style('fill', 'url(#lines)')
            .transition()
            .attr('width', (d: parallelPefromanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 

        var mtoThreadItems = mtoItems.selectAll('.mto_thread_item')
            .data(d => d.BatchedOperations)
            .enter()
            .append('g')
            .attr('class', 'mto_thread_item')
            .attr("transform",
            (d: parallelBatchStatsDto, i) =>
                "translate(0," + (i * self.yBarHeight / d.Parent.NumberOfThreads + self.yBarMargin) + ")");

        mtoThreadItems.selectAll('.mto_item')
            .data(d => d.Operations)
            .enter()
            .append('rect')
            .attr('class', (d: performanceStatsDto) => 'mto_item ' + d.Name)
            .attr('x', (d: performanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('y', 0)
            .attr('width', 0)
            .attr('height', (d: performanceStatsDto) => self.yBarHeight / d.ParallelParent.Parent.NumberOfThreads)
            .transition()
            .attr('width', (d: performanceStatsDto) => self.xScaleExtent(d.CacheWidth)); 
    }

    private updateDeletionOperations() {
        var self = this;
        var batches = self.svg.select('.deletion_group').select(".ops").selectAll(".op")
            .data(self.deletionJsonData, d => d.Id); 

        batches.exit().remove();

        var opTransition = batches
            .transition()
            .attr("transform", (d: deletionBatchInfoDto) => "translate(" + self.xScale(d.StartedAtDate) + "," + self.yDeletionScale(d.IndexName) + ")");

        opTransition.select('.main_bar')
            .attr('width', (d: deletionBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));

        opTransition.selectAll('.sto_item')
            .attr('x', (d: deletionPerformanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('width', (d: deletionPerformanceStatsDto) => self.xScaleExtent(d.CacheWidth));

        var enteringOps = batches
            .enter()
            .append('g')
            .attr('class', 'op')
            .on('click', self.deletionBatchInfoClicked.bind(self))
            .attr("transform", (d: deletionBatchInfoDto) =>
                "translate(" + self.xScale(d.StartedAtDate) + "," + self.yDeletionScale(d.IndexName) + ")");

        enteringOps.append('rect')
            .attr('class', 'main_bar')
            .attr('x', 0)
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .style('fill', (d: deletionBatchInfoDto) => self.color(d.StartedAt))
            .transition()
            .attr('width', (d: deletionBatchInfoDto) => self.xScaleExtent(d.TotalDurationMs));

        enteringOps.selectAll('.sto_item')
            .data((d: deletionBatchInfoDto) => d.PerformanceStats, d => d.Name)
            .enter()
            .append('rect')
            .attr('class', (d: deletionPerformanceStatsDto) => 'sto_item ' + d.Name)
            .attr('x', (d: deletionPerformanceStatsDto) => self.xScaleExtent(d.CacheCumulativeSum))
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .transition()
            .attr('width', (d: deletionPerformanceStatsDto) => self.xScaleExtent(d.CacheWidth));
    }

    private updatePrefetchOperations() {
        var self = this; 
        var batches = self.svg.select('.prefetch_group')
            .selectAll(".prefetch_op")
            .data(self.prefetchJsonData, d => d.CacheTimestamp);

        batches.exit().remove();

        batches
            .select('rect')
            .transition()
            .attr('x', d => self.xScale(d.CacheTimestamp))
            .attr('width', d => self.xScaleExtent(d.DurationMs));

        var enteringOps = batches
            .enter()
            .append('g')
            .attr('class', 'prefetch_op');

        enteringOps
            .append('rect')
            .attr('class', 'prefetch_box')
            .attr('x', d => self.xScale(d.CacheTimestamp))
            .attr('y', self.yBarMargin)
            .attr('width', 0)
            .attr('height', self.yBarHeight)
            .on('click', self.prefetchStatClicked.bind(self))
            .transition()
            .attr('width', d => self.xScaleExtent(d.DurationMs));
    }

    updateFilteredOutIndexesOperations() {
        var self = this;
        var batches = self.svg.select('.filtered_out_group')
            .selectAll('.filter_op')
            .data(self.filteredOutIndexesJsonData, d => this.createFilteredOutCacheKey(d));

        batches.exit().remove();

        var radius = 5;

        batches
            .transition()
            .attr('cx', d => self.xScale(d.TimestampParsed))
            .attr('cy', d => self.yMapScale(d.IndexName) + self.yBarHeight / 2 + radius / 2 + self.yBarMargin);
            
        batches
            .enter()
            .append('circle')
            .attr('class', 'filter_op')
            .attr('r', 0)
            .attr('cx', d => self.xScale(d.TimestampParsed))
            .attr('cy', d => self.yMapScale(d.IndexName) + self.yBarHeight / 2 + radius / 2 + self.yBarMargin)
            .style('stroke', 'black')
            .on('click', self.filterOutIndexClicked.bind(self))
            .transition()
            .attr('r', radius);
    }

    private updateGaps(gapsPositions: timeGap[]) {
        var self = this;
        var gaps = self.svg.select('.gaps').selectAll('.gap')
            .data(gapsPositions);

        gaps.exit().remove();

        var patternWidth = 20;
        var gapHeight = (self.selectedMapIndexNames().length + self.selectedReduceIndexes().length + self.selectedDeletionIndexes().length + 1 /* prefetch */) * (self.yBarHeight + self.yBarMargin * 2) + self.margin.between * 3;

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

    private parseTimespan(span: string) {
        var dotSplit = span.split(".");
        var milis = 0;
        if (dotSplit.length === 2) {
            milis = parseInt(dotSplit[1].substr(0, 3).replace("/^(0+)", ""));
            span = dotSplit[0];
        }

        var tokens = span.split(":").reverse();
        if (tokens.length > 0) {
            milis += parseInt(tokens[0]) * 1000;
        }
        if (tokens.length > 1) {
            milis += parseInt(tokens[1]) * 1000 * 60;
        }
        if (tokens.length > 2) {
            milis += parseInt(tokens[2]) * 1000 * 60 * 60;
        }

        return milis;
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

    private findMaxThreadCountInReduce(data: reduceLevelPeformanceStatsDto) {
        var parallelOps = <parallelPefromanceStatsDto[]>data.Operations.filter(x => "NumberOfThreads" in x);
        return d3.max(parallelOps.map(p => p.NumberOfThreads));
    }

    private showTooltip(data: any, templateName: string) {
        var container = document.getElementById("indexingPerformance");
        nv.tooltip.cleanup();
        var html = '<div data-bind="template: { name : \'' + templateName + '\' }"></div>';
        var clickLocation = d3.mouse(container);
        nv.tooltip.show([clickLocation[0], clickLocation[1]], html, 'n', 0, container, "selectable-tooltip");
        var tool = $(".nvtooltip");
        var node = tool[0];
        ko.applyBindings({ data: data, tooltipClose: nv.tooltip.cleanup }, node);

        var drag = d3.behavior.drag()
            .on('drag', function () {
                var o = tool.offset();
                tool.offset({ top: o.top + d3.event.dy, left: o.left + d3.event.dx });
            });

        d3.select('.nvtooltip').call(drag);

    }

    private filterOutIndexClicked(data: filteredOutIndexStatDto) {
        app.showDialog(new filteredOutIndexInfo(data));
    }

    private indexStatClicked(data: indexNameAndMapPerformanceStats) {
        data.CacheThreadCount = this.findMaxThreadCountInMap(data);
        app.showDialog(new mapStatsInfo(data));
    }

    private reduceStatClicked(data: reduceLevelPeformanceStatsDto) {
        data.CacheThreadCount = this.findMaxThreadCountInReduce(data);
        app.showDialog(new reduceStatsInfo(data));
    }

    private mapBatchInfoClicked(data: indexingBatchInfoDto) {
        app.showDialog(new mapBatchInfo(data));
    }

    private reduceBatchInfoClicked(data: indexingBatchInfoDto) {
        app.showDialog(new reduceBatchInfo(data));
    }

    private deletionBatchInfoClicked(data: deletionBatchInfoDto) {
        app.showDialog(new deletionBatchInfo(data)); 
    }

    private prefetchStatClicked(data: futureBatchStatsDto) {
        app.showDialog(new prefetchInfo(data));
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

    toggleFullscreen() {
        if ($(document).fullScreen()) {
            $("#indexingPerformance").width('').height('');
        } else {
            $("#indexingPerformance").width("100%").height("100%");
        }

        $("#indexingPerformance").toggleFullScreen();
    }
}

export = metrics; 
