import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

import lineChart = require("models/resources/clusterDashboard/lineChart");
import serverTraffic = require("models/resources/widgets/serverTraffic");

interface trafficState {
    showWritesDetails: boolean;
    showDataWrittenDetails: boolean;
}

class trafficWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload, void, trafficState> {

    showWritesDetails = ko.observable<boolean>(false);
    showDataWrittenDetails = ko.observable<boolean>(false);
    
    requestsChart: lineChart;
    writesChart: lineChart;
    dataWrittenChart: lineChart;
    
    nodeStats = ko.observableArray<serverTraffic>([]);
    
    constructor(controller: clusterDashboard, state: trafficState = undefined) {
        super(controller, undefined, state);
        
        _.bindAll(this, "toggleWritesDetails", "toggleDataWrittenDetails");

        for (const node of this.controller.nodes()) {
            const stats = new serverTraffic(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "Traffic";
    }

    getState(): trafficState {
        return {
            showDataWrittenDetails: this.showDataWrittenDetails(),
            showWritesDetails: this.showWritesDetails()
        }
    }

    restoreState(state: trafficState) {
        this.showWritesDetails(state.showWritesDetails);
        this.showDataWrittenDetails(state.showDataWrittenDetails);
    }
    
    compositionComplete() {
        super.compositionComplete();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
        
        this.initCharts();
        this.enableSyncUpdates();
    }

    initTooltip() {
        $('[data-toggle="tooltip"]', this.container).tooltip();
    }
    
    private initCharts() {
        const requestsContainer = this.container.querySelector(".requests-chart");
        this.requestsChart = new lineChart(requestsContainer, {
            grid: true,
            fillData: true
        });
        
        const writesChartContainer = this.container.querySelector(".writes-chart");
        this.writesChart = new lineChart(writesChartContainer, {
            grid: true,
            fillData: true
        });
        
        const dataWrittenContainer = this.container.querySelector(".data-written-chart");
        this.dataWrittenChart = new lineChart(dataWrittenContainer, {
            grid: true,
            fillData: true
        });
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.TrafficWatchPayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));
        
        const date = moment.utc(data.Date).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();
        
        this.scheduleSyncUpdate(() => {
            this.requestsChart.onData(date, [{
                key,
                value: data.RequestsPerSecond
            }]);

            this.writesChart.onData(date, [{
                key,
                value: data.DocumentWritesPerSecond
            }]);

            this.dataWrittenChart.onData(date, [{
                key,
                value: data.DocumentsWriteBytesPerSecond
            }]);
        });
    }

    protected afterSyncUpdate(updatesCount: number) {
        if (updatesCount) {
            this.requestsChart.draw();
            this.writesChart.draw();
            this.dataWrittenChart.draw();
        }
    }

    protected afterComponentResized() {
        this.requestsChart.onResize();
        this.writesChart.onResize();
        this.dataWrittenChart.onResize();
        
        this.requestsChart.draw();
        this.writesChart.draw();
        this.dataWrittenChart.draw();
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);
        
        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }
    
    private withStats(nodeTag: string, action: (stats: serverTraffic) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }

    toggleWritesDetails() {
        this.showWritesDetails.toggle();

        this.controller.layout(true, "shift");
    }

    toggleDataWrittenDetails() {
        this.showDataWrittenDetails.toggle();

        this.controller.layout(true, "shift");
    }
}

export = trafficWidget;
