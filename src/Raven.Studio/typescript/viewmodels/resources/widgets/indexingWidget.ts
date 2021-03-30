import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import indexingSpeed = require("models/resources/widgets/indexingSpeed");

class indexingWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload> {

    indexedPerSecondChart: lineChart;
    mappedPerSecondChart: lineChart;
    reducedPerSecondChart: lineChart;
    
    nodeStats = ko.observableArray<indexingSpeed>([]);
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new indexingSpeed(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "Indexing";
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
        const indexedPerSecondContainer = this.container.querySelector(".indexed-per-second-chart");
        this.indexedPerSecondChart = new lineChart(indexedPerSecondContainer, {
            grid: true
        });
        const mappedPerSecondContainer = this.container.querySelector(".mapped-per-second-chart");
        this.mappedPerSecondChart = new lineChart(mappedPerSecondContainer, {
            grid: true
        });
        const reducedPerSecondContainer = this.container.querySelector(".reduced-per-second-chart");
        this.reducedPerSecondChart = new lineChart(reducedPerSecondContainer, {
            grid: true
        });
    }
    
    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.IndexingSpeedPayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));

        const date = moment.utc(data.Date).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();
        
        this.scheduleSyncUpdate(() => {
            this.indexedPerSecondChart.onData(date, [{
                key,
                value: data.IndexedPerSecond
            }]);

            this.mappedPerSecondChart.onData(date, [{
                key,
                value: data.MappedPerSecond
            }]);

            this.reducedPerSecondChart.onData(date, [{
                key,
                value: data.ReducedPerSecond
            }]);
        });
    }

    protected afterSyncUpdate(updatesCount: number) {
        this.indexedPerSecondChart.draw();
        this.mappedPerSecondChart.draw();
        this.reducedPerSecondChart.draw();
    }

    protected afterComponentResized() {
        this.indexedPerSecondChart.onResize();
        this.mappedPerSecondChart.onResize();
        this.reducedPerSecondChart.onResize();

        this.indexedPerSecondChart.draw();
        this.mappedPerSecondChart.draw();
        this.reducedPerSecondChart.draw();
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: indexingSpeed) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = indexingWidget;
