import viewModelBase = require("viewmodels/viewModelBase");
import getDebugMetricsCommand = require("commands/database/debug/getDebugMetricsCommand");
import d3 = require("d3");
import nv = require("nvd3");

class requestsCount extends viewModelBase {
    /*
    currentMetrics: KnockoutObservable<statusDebugMetricsDto> = ko.observable(null);
    requestsMetricsUrl = ko.observable("");
    requestCounterChart: any = null; 
    requestCounterChartData: { [index: number]: string; key: string; values: any[] }[] = [
        {
            key: "Mean",
            values: []
        },
        {
            key: "1 min",
            values: []
        },
        {
            key: "5 min",
            values: []
        },
        {
            key: "15 min",
            values: []
        }
    ];

    modelPolling() {
        var deferred = $.Deferred();
        this.fetchMetrics().then(() => {
            this.appendData();
            this.updateGraph();
            deferred.resolve();
        });
        return deferred;
    }

    compositionComplete() {
        this.modelPolling();
    }

    canDeactivate() {
        window.onresize = null; // FIX nvd3 event attached globally
        return true;
    }

    appendData() {
        var metric = this.currentMetrics();
        var now = new Date();
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
    }

    updateGraph() {
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

    fetchMetrics(): JQueryPromise<statusDebugMetricsDto> {
        var db = this.activeDatabase();
        if (db) {
            var command = new getDebugMetricsCommand(db);
            this.requestsMetricsUrl(command.getQueryUrl());
            return command
                .execute()
                .done((m: statusDebugMetricsDto) => this.currentMetrics(m)); 
        }

        return null;
    }*/
}

export = requestsCount; 
