/// <reference path="../../Scripts/typings/d3/nvd3.d.ts" />
/// <reference path="../../Scripts/typings/d3/d3.d.ts" />
/// <reference path="../../Scripts/typings/d3/timelinesChart.d.ts" />
/// <reference path="../../Scripts/typings/d3/timelines.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import getDatabaseStatsCommand = require("commands/getDatabaseStatsCommand");
import d3 = require('d3/d3');
import nv = require('nvd3');

class metricsIndexBatchSize extends viewModelBase {

    currentStats: KnockoutObservable<databaseStatisticsDto> = ko.observable(null);
    indexBatchSizeQueryUrl = ko.observable("");

    batchSizeChart: any = null;
    batchSizeChartData = [
        {
            key: 'Batch size',
            values: []
        }
    ];

    attached() {
        this.modelPolling();
    }

    modelPolling() {
        this.fetchStats().then(() => {
            this.appendData();
            this.updateGraph();
        });
    }

    appendData() {
        var stats = this.currentStats();
        var batchInfos = stats.IndexingBatchInfo;
        var values = this.batchSizeChartData[0].values;

        for (var i = 0; i < batchInfos.length; i++) {
            var item = {
                x: new Date(batchInfos[i].StartedAt),
                y: batchInfos[i].TotalDocumentSize,
                size: batchInfos[i].TotalDocumentSize
            }
            var match = values.first(e => e.x.getTime() == item.x.getTime() && e.y == item.y);
            if (!match) {
                values.push(item);
            }
        }
    }

    updateGraph() {
        if (this.batchSizeChart === null) {
            nv.addGraph(function () {
                var chart = nv.models.scatterChart()
                    .margin({ left: 130 })
                    .transitionDuration(350)
                    .showLegend(true)
                    .showYAxis(true)
                    .showXAxis(true)
                    .forceY([0]);
                ;

                chart.xAxis
                    .axisLabel('Time')
                    .tickFormat(function (d) { return d3.time.format('%H:%M:%S')(new Date(d)); });

                chart.yAxis
                    .axisLabel('batch size')
                    .tickFormat(d3.format(',f'));

                nv.utils.windowResize(function () { chart.update() });
                return chart;
            }, (chart) => {
                    this.batchSizeChart = chart;
                    d3.select('#actualIndexingBatchSizeContainer svg')
                        .datum(this.batchSizeChartData)
                        .call(this.batchSizeChart);
                });
        } else {
            // just update data
            d3.select('#actualIndexingBatchSizeContainer svg')
                .datum(this.batchSizeChartData)
                .call(this.batchSizeChart);
        }
    }

    fetchStats(): JQueryPromise<databaseStatisticsDto> {
        var db = this.activeDatabase();
        if (db) {
            var command = new getDatabaseStatsCommand(db);
            this.indexBatchSizeQueryUrl(command.getQueryUrl());
            return command
                .execute()
                .done((s: databaseStatisticsDto) => this.currentStats(s));
        }
        return null;
    }
}

export = metricsIndexBatchSize; 
