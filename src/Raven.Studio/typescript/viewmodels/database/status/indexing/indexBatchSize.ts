import viewModelBase = require("viewmodels/viewModelBase");
import getIndexingBatchStatsCommand = require("commands/database/debug/getIndexingBatchStatsCommand");
import d3 = require('d3');
import nv = require('nvd3');

class metricsIndexBatchSize extends viewModelBase {
    /*
    currentStats: KnockoutObservable<indexingBatchInfoDto[]> = ko.observable(null);
    indexBatchSizeQueryUrl = ko.observable("");

    batchSizeChart: any = null;
    batchSizeChartData = [
        {
            key: 'Batch size',
            values: []
        }
    ];

    compositionComplete() {
        this.modelPolling();
    }

    detached() {
        super.detached();
        window.onresize = null; // FIX nvd3 event attached globally
    }

    modelPolling() {
        return this.fetchStats().then(() => {
            this.appendData();
            this.updateGraph();
        });
    }

    appendData() {
        var batchInfos = this.currentStats();
        var values = this.batchSizeChartData[0].values;

        for (var i = 0; i < batchInfos.length; i++) {
            var item = {
                x: new Date(batchInfos[i].StartedAt),
                y: batchInfos[i].TotalDocumentSize,
                size: batchInfos[i].TotalDocumentSize
            }
            var match = values.first(e => e.x.getTime() === item.x.getTime() && e.y === item.y);
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
                    .axisLabel('batch size [bytes]')
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

    fetchStats(): JQueryPromise<indexingBatchInfoDto[]> {
        var db = this.activeDatabase();
        if (db) {
            var command = new getIndexingBatchStatsCommand(db);
            this.indexBatchSizeQueryUrl(command.getQueryUrl());
            return command
                .execute()
                .done((s: indexingBatchInfoDto[]) => this.currentStats(s));
        }
        return null;
    }*/
}

export = metricsIndexBatchSize; 
