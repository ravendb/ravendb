import viewModelBase = require("viewmodels/viewModelBase");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import getStatusDebugConfigCommand = require("commands/database/debug/getStatusDebugConfigCommand");
import appUrl = require("common/appUrl");
import performanceTestRequest = require("models/database/debug/performanceTestRequest");
import performanceTestResultWrapped = require("models/database/debug/performanceTestResultWrapped");
import ioTestCommand = require("commands/database/debug/ioTestCommand");
import killRunningTaskCommand = require("commands/operations/killRunningTaskCommand");
import d3 = require('d3/d3');
import nv = require('nvd3');
import shell = require("viewmodels/shell");

class ioTest extends viewModelBase {

    isBusy = ko.observable<boolean>(false);
    ioTestRequest: performanceTestRequest = performanceTestRequest.empty();
    testResult = ko.observable<performanceTestResultWrapped>();

    currentStatus = ko.observable<string>();
    lastCommand: ioTestCommand = null;

    chunkSizeCustomValidityError: KnockoutComputed<string>;

    fileSizeMb = ko.computed({
        read: () => this.ioTestRequest.fileSize() / 1024 / 1024,
        write: (value:number) => this.ioTestRequest.fileSize(value * 1024 * 1024)
    });

    chunkSizeKb = ko.computed({
        read: () => this.ioTestRequest.chunkSize() / 1024,
        write: (value:number) => this.ioTestRequest.chunkSize(value * 1024)
    });

    overTimeThroughputChart: any = null;
    overTimeThroughputChartData = [];

    overTimeLatencyChart: any = null;
    overTimeLatencyChartData = [];
    isForbidden = ko.observable<boolean>();

    constructor() {
        super();

        this.ioTestRequest.sequential(false);

        this.chunkSizeCustomValidityError = ko.computed(() => {
            var errorMessage: string = '';
            if (isNaN(this.chunkSizeKb()) || this.chunkSizeKb() % 4 != 0) {
                errorMessage = "Chunk size must be multiple of 4";
            }
            return errorMessage;
        });

        this.isForbidden(shell.isGlobalAdmin() === false);
    }

    canActivate(args): any {
        var deferred = $.Deferred();

        if (this.isForbidden() === false) {
            new getStatusDebugConfigCommand(appUrl.getSystemDatabase())
                .execute()
                .done((results: any) =>
                    this.ioTestRequest.threadCount(results.MaxNumberOfParallelProcessingTasks))
                .always(() => deferred.resolve({ can: true }));
        } else {
            deferred.resolve({ can: true });
        }
        
        return deferred;
    }

    activate(args) {
        super.activate(args);
        this.updateHelpLink('ZL2H8E');
    }

    onIoTestCompleted(result: diskPerformanceResultWrappedDto) {
        this.testResult(new performanceTestResultWrapped(result)); 

        this.overTimeThroughputChartData = [];
        this.overTimeLatencyChartData = [];
        if (this.testResult().hasReads()) {
            this.overTimeThroughputChartData.push({
                key: 'Read throughput',
                values: result.Result.ReadPerSecondHistory.map((v, idx) => { return { x : idx, y: v / 1024 / 1024} } )
            });

            this.overTimeLatencyChartData.push({
                key: 'Read latency',
                values: result.Result.AverageReadLatencyPerSecondHistory.map((v, idx) => { return { x: idx, y: v } })
            });
        }

        if (this.testResult().hasWrites()) {
            this.overTimeThroughputChartData.push({
                key: 'Write throughput',
                values: result.Result.WritePerSecondHistory.map((v, idx) => { return { x : idx, y: v / 1024 / 1024} } )
            });

            this.overTimeLatencyChartData.push({
                key: 'Write latency',
                values: result.Result.AverageWriteLatencyPerSecondHistory.map((v, idx) => { return { x: idx, y: v } })
            });
        }

        if (this.overTimeLatencyChart === null) {
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
                    .axisLabel('Time [s]');

                chart.yAxis
                    .axisLabel('ms')
                    .tickFormat(d3.format(',.2f'));

                $(window).on('resize.ioTest', (e) => chart.update());

                return chart;
            }, (chart) => {
                this.overTimeLatencyChart = chart;
                    d3.select('#overTimeLatencyContainer svg')
                        .datum(this.overTimeLatencyChartData)
                        .call(this.overTimeLatencyChart);
                });
        } else {
            d3.select('#overTimeLatencyContainer svg')
                .datum(this.overTimeLatencyChartData)
                .call(this.overTimeLatencyChart);
        }

        if (this.overTimeThroughputChart === null) {
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
                    .axisLabel('Time [s]');

                chart.yAxis
                    .axisLabel('MB/sec')
                    .tickFormat(d3.format(',.2f'));

                nv.utils.windowResize(function () { chart.update() });
                return chart;
            }, (chart) => {
                    this.overTimeThroughputChart = chart;
                    d3.select('#overTimeThroughputContainer svg')
                        .datum(this.overTimeThroughputChartData)
                        .call(this.overTimeThroughputChart);
                });
        } else {
            d3.select('#overTimeThroughputContainer svg')
                .datum(this.overTimeThroughputChartData)
                .call(this.overTimeThroughputChart);
        }
    }

    killTask() {
        if (this.lastCommand !== null) {
            this.lastCommand.operationIdTask.done((operationId) => {
                new killRunningTaskCommand(appUrl.getSystemDatabase(), operationId).execute();
            });
        }
    }

    startPerformanceTest() {
        this.isBusy(true);
        var self = this;

        var diskTestParams = this.ioTestRequest.toDto();

            this.lastCommand = new ioTestCommand(appUrl.getSystemDatabase(), diskTestParams, s => { if (s) this.currentStatus(s.Progress); });
        this.lastCommand
            .execute()
            .done(() => {
                new getDocumentWithMetadataCommand("Raven/Disk/Performance", appUrl.getSystemDatabase())
                    .execute()
                    .done((result: diskPerformanceResultWrappedDto) => {
                        this.onIoTestCompleted(result);
                    });
            })
            .always(() => this.isBusy(false));
    }

    detached() {
        super.detached();
        window.onresize = null; // FIX nvd3 event attached globally
    }
}

export = ioTest;  
