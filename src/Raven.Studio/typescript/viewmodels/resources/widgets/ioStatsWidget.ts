
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import abstractChartsWebsocketWidget = require("viewmodels/resources/widgets/abstractChartsWebsocketWidget");
import ioStats = require("models/resources/widgets/ioStats");
import IoStatsResult = Raven.Client.ServerWide.Operations.IoStatsResult;
import app = require("durandal/app");
import ioStatsWidgetSettings = require("./settings/ioStatsWidgetSettings");
import { lineChart, chartData } from "models/resources/clusterDashboard/lineChart";

interface ioStatsWidgetConfig {
    splitIops?: boolean;
    splitThroughput?: boolean;
}

class ioStatsWidget extends abstractChartsWebsocketWidget<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload, ioStats, ioStatsWidgetConfig> {
    iopsChart: lineChart;
    iopsReadChart: lineChart;
    iopsWriteChart: lineChart;
    throughputChart: lineChart;
    throughputReadChart: lineChart;
    throughputWriteChart: lineChart;
    diskQueueChart: lineChart;

    splitIops = ko.observable<boolean>(false);
    splitThroughput = ko.observable<boolean>(false);
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new ioStats(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "IoStats";
    }

    openWidgetSettings(): void {
        const openSettingsDialog = new ioStatsWidgetSettings({ 
            splitIops: this.splitIops(),
            splitThroughput: this.splitThroughput()
        });

        app.showBootstrapDialog(openSettingsDialog)
            .done((result) => {
                this.splitIops(result.splitIops);
                this.splitThroughput(result.splitThroughput);
                
                this.afterComponentResized();
                this.controller.saveToLocalStorage();
                this.controller.layout(true, "shift");
            });
    }

    protected initCharts() {
        const chartsOpts = {
            grid: true,
            topPaddingProvider: () => 2,
            tooltipProvider: (date: Date) => ioStatsWidget.tooltipContent(date),
            onMouseMove: (date: Date) => this.onMouseMove(date)
        };
        
        let maxKnownIops = 0;
        
        const iopsCommonYProvider = (allCharts: chartData[]) => {
            maxKnownIops = Math.max(maxKnownIops, d3.max(allCharts.map(data => d3.max(data.ranges.filter(range => range.values.length).map(range => d3.max(range.values.map(values => values.y)))))));
            return maxKnownIops;
        }
        
        let maxKnownThroughput = 0;

        const throughputCommonYProvider = (allCharts: chartData[]) => {
            maxKnownThroughput = Math.max(maxKnownThroughput, d3.max(allCharts.map(data => d3.max(data.ranges.filter(range => range.values.length).map(range => d3.max(range.values.map(values => values.y)))))));
            return maxKnownThroughput;
        }
        
        this.iopsChart = new lineChart(this.container.querySelector(".disk-iops-line-chart"), chartsOpts);
        this.iopsReadChart = new lineChart(this.container.querySelector(".disk-iops-read-line-chart"), { 
            ...chartsOpts, 
            yMaxProvider: iopsCommonYProvider
        });
        this.iopsWriteChart = new lineChart(this.container.querySelector(".disk-iops-write-line-chart"), { 
            ...chartsOpts, 
            yMaxProvider: iopsCommonYProvider
        });
        this.throughputChart = new lineChart(this.container.querySelector(".disk-throughput-line-chart"), chartsOpts);
        this.throughputReadChart = new lineChart(this.container.querySelector(".disk-throughput-read-line-chart"), {
            ...chartsOpts,
            yMaxProvider: throughputCommonYProvider
        });
        this.throughputWriteChart = new lineChart(this.container.querySelector(".disk-throughput-write-line-chart"), {
            ...chartsOpts,
            yMaxProvider: throughputCommonYProvider
        });
        this.diskQueueChart = new lineChart(this.container.querySelector(".disk-queue-line-chart"), chartsOpts);
        
        return [
            this.iopsChart,
            this.iopsReadChart,
            this.iopsWriteChart,
            this.throughputChart,
            this.throughputReadChart,
            this.throughputWriteChart,
            this.diskQueueChart
        ];
    }
    
    protected extractDataForChart(chart: lineChart, data: Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload): number | undefined {
        const sumUp = (extractor: (item: IoStatsResult) => number) => {
            if (!data.Items.length) {
                return undefined;
            }
            return data.Items.map(extractor).reduce((p, c) => p + c, 0);
        };
        
        if (chart === this.iopsChart) {
            return sumUp(x => x.IoReadOperations + x.IoWriteOperations);
        } else if (chart === this.iopsReadChart) {
            return sumUp(x => x.IoReadOperations);
        } else if (chart === this.iopsWriteChart) {
            return sumUp(x => x.IoWriteOperations);
        } else if (chart === this.throughputChart) {
            return sumUp(x => x.ReadThroughputInKb + x.WriteThroughputInKb);
        } else if (chart === this.throughputReadChart) {
            return sumUp(x => x.ReadThroughputInKb);
        } else if (chart === this.throughputWriteChart) {
            return sumUp(x => x.WriteThroughputInKb);
        } else if (chart === this.diskQueueChart) {
            return sumUp(x => x.QueueLength ?? 0);
        } else {
            throw new Error("Unsupported chart: " + chart);
        }
    }
    
    getConfiguration(): ioStatsWidgetConfig {
        return {
            splitIops: this.splitIops(),
            splitThroughput: this.splitThroughput()
        }
    }
    
    restoreConfiguration(config: ioStatsWidgetConfig) {
        this.splitIops(config.splitIops);
        this.splitThroughput(config.splitThroughput);
    }
}

export = ioStatsWidget;
