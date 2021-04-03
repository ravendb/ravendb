import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import cpuUsage = require("models/resources/widgets/cpuUsage");

class cpuUsageWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload> {
   
    ravenChart: lineChart;
    serverChart: lineChart;
    
    nodeStats = ko.observableArray<cpuUsage>([]);
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new cpuUsage(node.tag());
            this.nodeStats.push(stats);
        }
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
            topPaddingProvider: () => 2
        });
        const serverChartContainer = this.container.querySelector(".machine-line-chart");
        this.serverChart = new lineChart(serverChartContainer, {
            grid: true,
            yMaxProvider: () => 100,
            topPaddingProvider: () => 2
        });
    }
    
    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.CpuUsagePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));

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

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: cpuUsage) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = cpuUsageWidget;
