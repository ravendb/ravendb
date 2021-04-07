import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import cpuUsage = require("models/resources/widgets/cpuUsage");

class cpuUsageWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload> {

    private readonly throttledShowHistory: (date: Date) => void;
    
    ravenChart: lineChart;
    serverChart: lineChart;
    
    nodeStats = ko.observableArray<cpuUsage>([]);
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new cpuUsage(node.tag());
            this.nodeStats.push(stats);
        }
        
        this.throttledShowHistory = _.throttle((d: Date) => this.showNodesHistory(d), 100);
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "CpuUsage";
    }
    
    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
        
        this.initCharts();
    }

    private initCharts() {
        const ravenChartContainer = this.container.querySelector(".ravendb-line-chart");
        this.ravenChart = new lineChart(ravenChartContainer, {
            grid: true,
            yMaxProvider: () => 100,
            topPaddingProvider: () => 2,
            tooltipProvider: date => cpuUsageWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const serverChartContainer = this.container.querySelector(".machine-line-chart");
        this.serverChart = new lineChart(serverChartContainer, {
            grid: true,
            yMaxProvider: () => 100,
            topPaddingProvider: () => 2,
            tooltipProvider: date => cpuUsageWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
    }
    
    private static tooltipContent(date: Date|null) {
        if (date) {
            const dateFormatted = moment(date).format(lineChart.timeFormat);
            return `<div class="tooltip-inner">Time: <strong>${dateFormatted}</strong></div>`;
        } else {
            return null;
        }
    }
    
    onMouseMove(date: Date|null) {
        this.ravenChart.highlightTime(date);
        this.serverChart.highlightTime(date);
        
        this.throttledShowHistory(date);
    }
    
    private showNodesHistory(date: Date|null) {
        this.nodeStats().forEach(nodeStats => {
            nodeStats.showItemAtDate(date);
        });
    }
    
    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.onData(data)));

        const date = moment.utc(data.Date).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();
        
        this.scheduleSyncUpdate(() => {
            this.ravenChart.onData(date, [{
                key,
                value: data.ProcessCpuUsage
            }]);

            this.serverChart.onData(date, [{
                key,
                value: data.MachineCpuUsage
            }]);
        });
    }

    protected afterSyncUpdate(updatesCount: number) {
        this.ravenChart.draw();
        this.serverChart.draw();
    }

    protected afterComponentResized() {
        this.ravenChart.onResize();
        this.serverChart.onResize();

        this.ravenChart.draw();
        this.serverChart.draw();
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(true));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(false));
    }

    private withStats(nodeTag: string, action: (stats: cpuUsage) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = cpuUsageWidget;
