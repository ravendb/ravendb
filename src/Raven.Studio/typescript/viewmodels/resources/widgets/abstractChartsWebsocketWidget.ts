import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import lineChart = require("models/resources/clusterDashboard/lineChart");

abstract class abstractChartsWebsocketWidget<
    TPayload extends Raven.Server.Dashboard.Cluster.AbstractClusterDashboardNotification, 
    TNodeStats extends historyAwareNodeStats<TPayload>,
    TConfig = unknown, 
    TState = unknown
    > extends websocketBasedWidget<TPayload, TConfig, TState> {

    protected readonly throttledShowHistory: (date: Date) => void;

    protected charts: lineChart[] = [];

    nodeStats = ko.observableArray<TNodeStats>([]);

    protected constructor(controller: clusterDashboard) {
        super(controller);

        this.throttledShowHistory = _.throttle((d: Date) => this.showNodesHistory(d), 100);
    }

    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }

        this.charts = this.initCharts();
    }

    protected static tooltipContent(date: Date | null) {
        if (date) {
            const dateFormatted = moment(date).format(lineChart.timeFormat);
            return `<div class="tooltip-inner">Time: <strong>${dateFormatted}</strong></div>`;
        } else {
            return null;
        }
    }

    protected abstract initCharts(): lineChart[];

    onMouseMove(date: Date | null) {
        this.charts.forEach(chart => chart.highlightTime(date));

        this.throttledShowHistory(date);
    }

    protected showNodesHistory(date: Date | null) {
        this.nodeStats().forEach(nodeStats => {
            nodeStats.showItemAtDate(date);
        });
    }

    protected withStats(nodeTag: string, action: (stats: TNodeStats) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(true, ws.connectedAt));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);
        
        // flush pending changes - as we redraw anyway 
        this.forceSyncUpdate();
        
        const now = new Date();

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(false));
        this.charts.forEach(chart => chart.recordNoData(now, abstractChartsWebsocketWidget.chartKey(ws.nodeTag)));
    }

    protected afterSyncUpdate(updatesCount: number) {
        this.charts.forEach(chart => chart.draw());
    }

    afterComponentResized() {
        super.afterComponentResized();
        this.charts.forEach(chart => chart.onResize());
        this.charts.forEach(chart => chart.draw());
    }
    
    private static chartKey(nodeTag: string) {
        return "node-" + nodeTag.toLocaleLowerCase();
    }

    onData(nodeTag: string, data: TPayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.onData(data)));

        const date = moment.utc(data.Date).toDate();

        this.scheduleSyncUpdate(() => {
            this.charts.forEach(chart => {
                chart.onData(date, [{
                    key: abstractChartsWebsocketWidget.chartKey(nodeTag),
                    value: this.extractDataForChart(chart, data)
                }]);
            })
        });
    }

    protected abstract extractDataForChart(chart: lineChart, data: TPayload): number;
}

export = abstractChartsWebsocketWidget;
