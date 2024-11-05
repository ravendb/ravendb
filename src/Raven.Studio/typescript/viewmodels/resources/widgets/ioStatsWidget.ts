
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import abstractChartsWebsocketWidget = require("viewmodels/resources/widgets/abstractChartsWebsocketWidget");
import ioStats = require("models/resources/widgets/ioStats");
import IoStatsResult = Raven.Client.ServerWide.Operations.IoStatsResult;
import app = require("durandal/app");
import ioStatsWidgetSettings = require("./settings/ioStatsWidgetSettings");
import d3 = require("d3");
import { lineChart, chartData } from "models/resources/clusterDashboard/lineChart";

interface ioStatsWidgetConfig {
    splitIops?: boolean;
    splitThroughput?: boolean;
}

class ioStatsWidget extends abstractChartsWebsocketWidget<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload, ioStats, ioStatsWidgetConfig> {

    view = require("views/resources/widgets/ioStatsWidget.html");
    
    iopsChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    iopsReadChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    iopsWriteChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    throughputChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    throughputReadChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    throughputWriteChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;
    diskQueueChart: lineChart<Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload>;

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

        const sumUp = (data: Raven.Server.Dashboard.Cluster.Notifications.IoStatsPayload, extractor: (item: IoStatsResult) => number) => {
            if (!data.Items.length) {
                return undefined;
            }
            return data.Items.map(extractor).reduce((p, c) => p + c, 0);
        };
        
        this.iopsChart = new lineChart(this.container.querySelector(".disk-iops-line-chart"),
            data => sumUp(data, x => x.IoReadOperations + x.IoWriteOperations),
            chartsOpts);
        this.iopsReadChart = new lineChart(this.container.querySelector(".disk-iops-read-line-chart"),
            data => sumUp(data, x => x.IoReadOperations),
            {
                ...chartsOpts,
                yMaxProvider: iopsCommonYProvider
            });
        this.iopsWriteChart = new lineChart(this.container.querySelector(".disk-iops-write-line-chart"),
            data => sumUp(data, x => x.IoWriteOperations),
            {
                ...chartsOpts,
                yMaxProvider: iopsCommonYProvider
            });
        this.throughputChart = new lineChart(this.container.querySelector(".disk-throughput-line-chart"),
            data => sumUp(data, x => x.ReadThroughputInKb + x.WriteThroughputInKb),
            chartsOpts);
        this.throughputReadChart = new lineChart(this.container.querySelector(".disk-throughput-read-line-chart"),
            data => sumUp(data, x => x.ReadThroughputInKb),
            {
                ...chartsOpts,
                yMaxProvider: throughputCommonYProvider
            });
        this.throughputWriteChart = new lineChart(this.container.querySelector(".disk-throughput-write-line-chart"),
            data => sumUp(data, x => x.WriteThroughputInKb),
            {
                ...chartsOpts,
                yMaxProvider: throughputCommonYProvider
            });
        this.diskQueueChart = new lineChart(this.container.querySelector(".disk-queue-line-chart"),
            data => sumUp(data, x => x.QueueLength ?? 0),
            chartsOpts);
        
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
