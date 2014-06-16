/// <reference path="../../Scripts/typings/d3/d3.d.ts" />
/// <reference path="../../Scripts/typings/nvd3/nvd3.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");


class metrics extends viewModelBase {

    static graphCache = {};

    attached() {
        this.fetchStats();
    }

    modelPolling() {
        this.fetchStats();
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            return new getDatabaseStatsCommand(db)
                .execute()
                .done((result: databaseStatisticsDto) => this.processStatsResults(result));
        }
        return null;
    }
    
    processStatsResults(results: databaseStatisticsDto) {
        var chartsContainer = d3.select('#indexPerfContainer').selectAll('.index-perf').data(results.Indexes, (d, i) => d.Id);
        var chartsEnter = chartsContainer.enter().append('div').attr('class', 'index-perf');
        chartsContainer.exit().remove();
        chartsContainer.call(metrics.chart);
    }

    static chart(selection: D3.Selection) {
        selection.each(function (data: indexStatisticsDto) {
            var container = d3.select(this);
            var wrap = container.selectAll('.timeChart').data([metrics.mapStatsData(data)]);

            if (nv.graphs.length > 0 && metrics.graphCache[data.PublicName] && metrics.graphCache[data.PublicName].autoUpdate) {
                wrap.call(metrics.graphCache[data.PublicName].chart);
            }

            wrap.enter().append('h3').attr('class', 'indexName').text(function (d, i) {
                metrics.graphCache[data.PublicName] = { autoUpdate : false };
                return data.PublicName;
            });
            var wrapEnter = wrap.enter().append('svg').attr('height', 500).attr('class', 'timeChart');

            wrapEnter.each(function (data: indexStatisticsDto) {
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
                        var indexName = d3.select(chart.container.parentNode).select('h3').text();
                        metrics.graphCache[indexName].autoUpdate = !!e.disabled;
                    });

                    wrap
                        .datum(data)
                        .call(chart);

                    nv.utils.windowResize(chart.update);

                    return chart;
                }, function (chart) {
                    var indexName = d3.select(chart.container.parentNode).select('h3').text();

                    metrics.graphCache[indexName].chart = chart;
                });
            });

        });
    }

    static mapStatsData(data: indexStatisticsDto) {
        var chartData = {};
        data.Performance.forEach(perfDto => {
            if (perfDto.Operation !== "Current Map") {
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
   
}

export = metrics; 