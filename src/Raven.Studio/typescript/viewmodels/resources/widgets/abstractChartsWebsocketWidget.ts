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

    protected constructor(controller: clusterDashboard, config: TConfig = undefined, state: TState = undefined) {
        super(controller, config, state);

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

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(true));
        
        this.charts.forEach(chart => {
            //TODO: chart.onConnectionStatusChanged(ws.nodeTag, true);
        });
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);
        //TODO: flush pending updates? 

        this.withStats(ws.nodeTag, x => x.onConnectionStatusChanged(false));
        this.charts.forEach(chart => {
            //TODO: chart.onConnectionStatusChanged(ws.nodeTag, false);
        });
    }

    protected afterSyncUpdate(updatesCount: number) {
        this.charts.forEach(chart => chart.draw());
    }

    protected afterComponentResized() {
        this.charts.forEach(chart => chart.onResize());
        this.charts.forEach(chart => chart.draw());
    }

    onData(nodeTag: string, data: TPayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.onData(data)));

        const date = moment.utc(data.Date).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();

        this.scheduleSyncUpdate(() => {
            this.charts.forEach(chart => {
                chart.onData(date, [{
                    key,
                    value: this.extractDataForChart(chart, data)
                }]);
            })
        });
    }

    protected abstract extractDataForChart(chart: lineChart, data: TPayload): number;
}

export = abstractChartsWebsocketWidget;
