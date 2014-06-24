/// <reference path="../../Scripts/typings/d3/nv.d.ts" />
/// <reference path="../../Scripts/typings/d3/d3.d.ts" />
/// <reference path="../../Scripts/typings/d3/timelinesChart.d.ts" />
/// <reference path="../../Scripts/typings/d3/timelines.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import getStatusDebugMetricsCommand = require("commands/getStatusDebugMetricsCommand");
import d3 = require('d3/d3');
import nv = require('d3/nv');
import timelines = require('d3/models/timelines');
import timelinesChart = require('d3/models/timelinesChart');

class metrics extends viewModelBase {

    currentStats: KnockoutObservable<databaseStatisticsDto> = ko.observable(null);
    currentMetrics: KnockoutObservable<statusDebugMetricsDto> = ko.observable(null);

    availableIndexes = ko.observableArray<indexStatisticsDto>();
    selectedIndex = ko.observable(null);
    allIndexPerfChartVisible = ko.computed(() => this.selectedIndex() === null);

    specificIndexChart: any = null;
    specificIndexChartData: any = [];
    static specificIndexAutoUpdate = false;

    allIndexPerfChart: any = null;
    allIndexPerfChartData = [
        {
            key: 'Index',
            values: []
        },
        {
            key: 'Reduce',
            values: []
        }
    ];

    requestCounterChart: any = null; 
    requestCounterChartData = [
        {
            key: 'Mean',
            values: []
        },
        {
            key: '1 min',
            values: []
        },
        {
            key: '5 min',
            values: []
        },
        {
            key: '15 min',
            values: []
        }
    ];

    attached() {
        metrics.specificIndexAutoUpdate = false;
    }

    currentIndexName: KnockoutComputed<string> = ko.computed(() => {
        if (this.selectedIndex() === null) {
            return "All indexes";
        }
        return this.selectedIndex();
    });

    modelPolling() {
        $.when(this.fetchStats(), this.fetchMetrics()).then(() => {
            this.fillIndexNames();
            this.appendData();
            this.updateGraph();
        });
    }

    appendData() {
        var metric = this.currentMetrics();
        var stats = this.currentStats();
        var now = new Date();
        this.allIndexPerfChartData[0].values.push({
            x: now,
            y: metric.IndexedPerSecond
        });
        this.allIndexPerfChartData[1].values.push({
            x: now,
            y: metric.ReducedPerSecond
        });
        this.requestCounterChartData[0].values.push({
            x: now,
            y: metric.Requests.MeanRate
        });
        this.requestCounterChartData[1].values.push({
            x: now,
            y: metric.Requests.OneMinuteRate
        });
        this.requestCounterChartData[2].values.push({
            x: now,
            y: metric.Requests.FiveMinuteRate
        });
        this.requestCounterChartData[3].values.push({
            x: now,
            y: metric.Requests.FifteenMinuteRate
        });
        this.appendIndexData();
    }

    appendIndexData() {
        var stats = this.currentStats();
        var currentStat = stats.Indexes.filter((indexStat) => indexStat.PublicName == this.currentIndexName());
        if (currentStat && currentStat.length == 1) {
            var newData = metrics.mapStatsData(currentStat[0].Performance);

            for (var i = 0; i < newData.length; i++) {
                var serie = newData[i];
                var key = serie.key;
                var values = serie.values;
                var match = this.specificIndexChartData.filter(x => x.key == key);
                if (match && match.length == 1) {
                    metrics.arrayMerge(match[0].values, serie.values);
                } else {
                    this.specificIndexChartData.push(newData[i]);
                }
            }
        }
    }

    static arrayMerge(target: any[], source: any[]) {
        for (var i = 0; i < source.length; i++) {
            var newItem = source[i];
            // try to find newItem in target array
            var match = target.filter(x => x.x == newItem.x && x.size == newItem.size);
            if (!match || match.length == 0) {
                target.push(newItem);
            }
        }
    }

    

    
    static mapStatsData(data: indexPerformanceDto[]) {
        var chartData = {};
        data.forEach(perfDto => {
            if (perfDto.Operation !== "Current Map" && perfDto.DurationMilliseconds > 0) {
                if (!chartData[perfDto.Operation]) {
                    chartData[perfDto.Operation] = {
                        key: perfDto.Operation,
                        values: []
                    };
                }
                chartData[perfDto.Operation].values.push({
                    x: new Date(perfDto.Started).getTime(),
                    y: perfDto.InputCount / perfDto.DurationMilliseconds * 1000,
                    size: perfDto.DurationMilliseconds,
                    payload: perfDto
                });
            }
        });
        return $.map(chartData, (v, idx) => v);
    }

    setSelectedIndex(indexName: string) {
        this.selectedIndex(indexName);
        this.specificIndexChartData = [];
        this.appendIndexData();
        this.updateIndexChart();
    }

    updateIndexChart() {
        if (this.allIndexPerfChartVisible()) {
            this.updateAllIndexChart();
        } else {
            this.updateSpecificIndexChart();
        }
    }

    updateGraph() {
        this.updateIndexChart();
        this.updateRequestCounterChart();
    }

    updateAllIndexChart() {
        if (this.allIndexPerfChart === null) {
            nv.addGraph(function () {
                var chart = nv.models.lineChart()
                    .margin({ left: 130 })
                    .useInteractiveGuideline(true)
                    .transitionDuration(350)
                    .showLegend(true)
                    .showYAxis(true)
                    .showXAxis(true)
                    .forceY([0]);
                ;

                chart.xAxis
                    .axisLabel('Time')
                    .tickFormat(function (d) { return d3.time.format('%H:%M:%S')(new Date(d)); });

                chart.xScale(d3.time.scale());

                chart.yAxis
                    .axisLabel('docs/sec')
                    .tickFormat(d3.format('.01f'));

                nv.utils.windowResize(function () { chart.update() });
                return chart;
            }, (chart) => {
                this.allIndexPerfChart = chart;
                d3.select('#allIndexesPerfContainer svg')
                    .datum(this.allIndexPerfChartData)
                    .call(this.allIndexPerfChart);
                });
        } else {
            d3.select('#allIndexesPerfContainer svg')
                .datum(this.allIndexPerfChartData)
                .call(this.allIndexPerfChart);
        }
    }

    updateSpecificIndexChart() {
        if (this.specificIndexChart === null) {
            nv.addGraph(function () {
                var chart = nv.models.timelinesChart()
                    .showDistX(true)
                    .showDistY(true)
                    .showControls(true)
                    .color(d3.scale.category10().range())
                    .transitionDuration(250)
                ;
                chart.yAxis.showMaxMin(false);
                chart.forceY([0]);
                chart.y2Axis.showMaxMin(false);
                chart.xAxis.showMaxMin(false);
                chart.x2Axis.showMaxMin(false);
                chart.xAxis.tickFormat(function (_) { return d3.time.format("%H:%M:%S")(new Date(_)); });
                chart.x2Axis.tickFormat(function (_) { return d3.time.format("%H:%M:%S")(new Date(_)); });

                chart.tooltipContent(function (key, x, y, data) {
                    return '<h4>' + key + '</h4>'
                        + 'Items count: ' + data.point.payload.ItemsCount + '<br />'
                        + 'Input count: ' + data.point.payload.InputCount + '<br />'
                        + 'Output count: ' + data.point.payload.OutputCount + '<br />'
                        + 'Started: ' + data.point.payload.Started + '<br />'
                        + 'Duration: ' + data.point.payload.Duration;
                });

                chart.dispatch.on('controlsChange', function (e) {
                    metrics.specificIndexAutoUpdate = !!e.disabled;
                });

                nv.utils.windowResize(chart);

                return chart;
            }, (chart) => {
                this.specificIndexChart = chart;
                d3.select('#indexPerfContainer svg')
                    .datum(this.specificIndexChartData)
                    .call(this.specificIndexChart);
                });
        } else {
            if (metrics.specificIndexAutoUpdate) {
                d3.select('#indexPerfContainer svg')
                    .datum(this.specificIndexChartData)
                    .call(this.specificIndexChart);
            }
        }
    }

    updateRequestCounterChart() {
        if (this.requestCounterChart === null) {
            nv.addGraph(function () {
                var chart = nv.models.lineChart()
                    .margin({ left: 130 })
                    .useInteractiveGuideline(true)
                    .transitionDuration(350)
                    .showLegend(true)
                    .showYAxis(true)
                    .showXAxis(true)
                    .forceY([0]);
                ;

                chart.xAxis
                    .axisLabel('Time')
                    .tickFormat(function (d) { return d3.time.format('%H:%M:%S')(new Date(d)); });

                chart.xScale(d3.time.scale());

                chart.yAxis
                    .axisLabel('request/sec')
                    .tickFormat(d3.format('.01f'));

                nv.utils.windowResize(function () { chart.update() });
                return chart;
            }, (chart) => {
                this.requestCounterChart = chart;
                d3.select('#requestCounterContainer svg')
                    .datum(this.requestCounterChartData)
                    .call(this.requestCounterChart);
                });
        } else {
            // just update data
            d3.select('#requestCounterContainer svg')
                .datum(this.requestCounterChartData)
                .call(this.requestCounterChart);
        }
    }

    fillIndexNames() {
        this.availableIndexes(this.currentStats().Indexes.filter(idx => idx.Performance.length > 0));
    }
    
    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute().done((s: databaseStatisticsDto) => this.currentStats(s));
        }
        return null;
    }

    fetchMetrics(): JQueryPromise<statusDebugMetricsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getStatusDebugMetricsCommand(db)
                .execute().done((m: statusDebugMetricsDto) => this.currentMetrics(m)); 
        }

        return null;
    }
}

export = metrics; 