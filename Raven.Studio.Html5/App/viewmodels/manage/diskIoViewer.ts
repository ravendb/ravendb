/// <reference path="../../../Scripts/typings/d3/nvd3.d.ts" />
/// <reference path="../../../Scripts/typings/d3/d3.d.ts" />

import viewModelBase = require("viewmodels/viewModelBase");
import d3 = require("d3/d3");
import nv = require("nvd3");
import appUrl = require("common/appUrl");
import listDiskPerformanceRunsCommand = require("commands/maintenance/listDiskPerformanceRunsCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import deleteDocumentCommand = require("commands/database/documents/deleteDocumentCommand");
import settingsAccessAuthorizer = require("common/settingsAccessAuthorizer");

class diskIoViewer extends viewModelBase {

    isoFormat = d3.time.format.iso;

    settingsAccess = new settingsAccessAuthorizer();

    showChart = ko.observable<boolean>(false);
    emptyReport = ko.observable<boolean>(false);

    performanceRuns = ko.observableArray<performanceRunItemDto>([]);
    currentPerformanceRun = ko.observable<performanceRunItemDto>();
    currentPeformanceRunLabel = ko.computed(() => {
        var run = this.currentPerformanceRun();
        if (run) {
            return run.displayName;
        }
        return "select";
    });

    perDbReports = ko.observableArray<diskIoPerformanceRunResultDto>([]);
    currentDbReport = ko.observable<diskIoPerformanceRunResultDto>();
    currentDbReportLabel = ko.computed(() => {
        var report = this.currentDbReport();
        if (report) {
            return report.Name;
        }
        return "database name";
    });

    data: diskIoPerformanceRunDto;

    chart: any = null; 
    chartData: { [index: number]: string; key: string; values: any[] }[] = [
        {
            key: "Index Read",
            values: []
        },
        {
            key: "Index Write",
            values: []
        },
        {
            key: "Data read",
            values: []
        },
        {
            key: "Data write",
            values: []
        }
    ];

    constructor() {
        super();
        this.currentPerformanceRun.subscribe(v => {
            if (!v) {
                this.perDbReports([]);
                this.currentDbReport(undefined);
                return;
            }
            new getDocumentWithMetadataCommand(v.documentId, appUrl.getSystemDatabase())
                .execute()
                .done((doc: diskIoPerformanceRunDto) => {
                    this.emptyReport(doc.Databases.length === 0);
                    this.showChart(doc.Databases.length > 0);
                    this.data = doc;
                    if (doc.Databases) {
                        this.perDbReports(doc.Databases);
                        if (doc.Databases.length > 0) {
                            this.currentDbReport(doc.Databases[0]);
                        } else {
                            this.currentDbReport(undefined);
                        }
                    } else {
                        this.perDbReports([]);
                    }
                });
        });

        this.currentDbReport.subscribe(v => {
            this.updateData();
            this.updateGraph();
        });
    }

    canActivate(args): any {
        var deffered = $.Deferred();

        new listDiskPerformanceRunsCommand()
            .execute() 
            .done((results: performanceRunItemDto[]) => {
                this.performanceRuns(results);
            })
            .always(() => deffered.resolve({ can: true }));

        return deffered;
    }

    activate(args) {
        super.activate(args);

        this.updateHelpLink("K6J4EE");
    }

    detached() {
        super.detached();
        window.onresize = null; // FIX nvd3 event attached globally
    }

    updateData() {
        for (var i = 0; i < 4; i++) {
            this.chartData[i].values = [];
        }
        if (!this.currentDbReport()) {
            // workaround for empty data
            for (var i = 0; i < 4; i++) {
                this.chartData[i].values.push({
                    x: new Date(),
                    y: 0
                });
            }   
            return;
        }

        var dbResults = this.currentDbReport().Results;
        for (var date in dbResults) {
            if (dbResults.hasOwnProperty(date)) {
                var value = dbResults[date];

                var parsedDate = this.isoFormat.parse(date);
                var yValues = [0, 0, 0, 0];

                for (var i = 0; i < value.length; i++) {
                    var item = value[i];

                    //find index in values array
                    if (item.WriteIoSizeInBytes > 0) {
                        if (item.PathType === "Data") {
                            yValues[3] = item.WriteIoSizeInBytes;
                        } else if (item.PathType === "Index") {
                            yValues[1] = item.WriteIoSizeInBytes;
                        }
                    }
                    if (item.ReadIoSizeInBytes > 0) {
                        if (item.PathType === "Data") {
                            yValues[2] = item.ReadIoSizeInBytes;
                        } else if (item.PathType === "Index") {
                            yValues[0] = item.ReadIoSizeInBytes;
                        }
                    }
                }

                for (var i = 0; i < yValues.length; i++) {
                    this.chartData[i].values.push({
                        x: parsedDate,
                        y: yValues[i] / 1024.0 / 1024.0
                    });
                }
            }
        }
    }

    updateGraph() {
        if (this.chart === null) {
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
                    .axisLabel('Mb')
                    .tickFormat(d3.format('.01f'));

                nv.utils.windowResize(function () { chart.update() });
                return chart;
            }, (chart) => {
                this.chart = chart;
                d3.select('#diskMonitorContainer svg')
                    .datum(this.chartData)
                    .call(this.chart);
                });
        } else {
            // just update data
            d3.select('#diskMonitorContainer svg')
                .datum(this.chartData)
                .call(this.chart);
        }
    }

    private switchReport(report: performanceRunItemDto) {
        this.currentPerformanceRun(report);
    }   
    
    private switchDatabase(report: diskIoPerformanceRunResultDto) {
        this.currentDbReport(report);
    }

    deleteReport() {
        this.confirmationMessage("Are you sure?", "You are removing Disk IO Report: " + this.currentPeformanceRunLabel())
            .done(() => {
                var currentRun = this.currentPerformanceRun();
                new deleteDocumentCommand(this.currentPerformanceRun().documentId, appUrl.getSystemDatabase())
                    .execute();
                this.performanceRuns.remove(currentRun);
                this.currentPerformanceRun(null);
                this.showChart(false);
            });
    }
}

export = diskIoViewer; 
