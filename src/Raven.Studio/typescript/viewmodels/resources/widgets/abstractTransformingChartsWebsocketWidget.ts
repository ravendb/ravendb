import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import historyAwareNodeStats = require("models/resources/widgets/historyAwareNodeStats");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import moment = require("moment");
import { lineChart } from "models/resources/clusterDashboard/lineChart";
import { clusterDashboardChart } from "models/resources/clusterDashboard/clusterDashboardChart";

abstract class abstractTransformingChartsWebsocketWidget<
    TPayload extends Raven.Server.Dashboard.Cluster.AbstractClusterDashboardNotification,
    TTransformedData extends { Date: string; },
    TChartData extends { Date: string; Key: string; },
    TNodeStats extends historyAwareNodeStats<TTransformedData>,
    TConfig = unknown,
    TState = unknown
> extends websocketBasedWidget<TPayload, TConfig, TState> {

    protected readonly throttledShowHistory: (date: ClusterWidgetAlignedDate) => void;

    protected charts: clusterDashboardChart<TChartData>[] = [];

    nodeStats = ko.observableArray<TNodeStats>([]);

    protected constructor(controller: clusterDashboard) {
        super(controller);

        this.throttledShowHistory = _.throttle((d: ClusterWidgetAlignedDate) => this.showNodesHistory(d, true), 100);
    }

    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();

        for (const ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }

        this.charts = this.initCharts();
    }

    protected static tooltipContent(date: ClusterWidgetUnalignedDate | null) {
        if (date) {
            const dateFormatted = moment(date).format(lineChart.timeFormat);
            return `<div class="tooltip-inner"><div class="tooltip-li">Time: <div class="value">${dateFormatted}</div></div></div>`;
        } else {
            return null;
        }
    }

    protected abstract initCharts(): clusterDashboardChart<TChartData>[];

    onMouseMove(date: Date | null) {
        this.charts.forEach(chart => chart.highlightTime(date));

        this.throttledShowHistory(date);
    }
    
    protected quantizeDate(date: ClusterWidgetUnalignedDate, dates: Date[]): ClusterWidgetAlignedDate {
        if (!date || !dates.length) {
            return null;
        }
        
        const distances = dates.map(dd => ({ domainDate: dd, distance: Math.abs(date.getTime() - dd.getTime())}));
        distances.sort((a, b) => a.distance - b.distance);
        const closestItem = distances[0];
        // 50 px * 500 ms/pixel = 25 seconds (snap)
        return closestItem.distance < 500 * 50 ? closestItem.domainDate as ClusterWidgetAlignedDate : null;
    }

    protected showNodesHistory(date: ClusterWidgetAlignedDate | null, fallbackToCurrent: boolean) {
        this.nodeStats().forEach(nodeStats => {
            nodeStats.showItemAtDate(date, fallbackToCurrent);
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
        this.charts.forEach(chart => chart.recordNoData(now, abstractTransformingChartsWebsocketWidget.chartKey(ws.nodeTag)));
    }

    protected afterSyncUpdate() {
        this.charts.forEach(chart => chart.draw());
    }

    afterComponentResized() {
        super.afterComponentResized();
        this.charts.forEach(chart => chart.onResize());
        this.charts.forEach(chart => chart.draw());
    }

    private static chartKey(nodeTag: string) {
        return "node-" + nodeTag;
    }

    onData(nodeTag: string, data: TPayload) {
        this.scheduleSyncUpdate(() => {
            const transformedData = this.transformSocketData(nodeTag, data);

            transformedData.forEach(item => {
                this.withStats(nodeTag, x => x.onData(item));
                
                const chartData = this.transformChartData(nodeTag, item);
                chartData.forEach(chartItem => {
                    this.charts.forEach(chart => {
                        if (this.canAppendToChart(chart, nodeTag, chartItem)) {
                            chart.onData(chartItem.Key, chartItem);
                        }
                    });
                });
            })
        });
    }
    
    protected canAppendToChart(chart: clusterDashboardChart<TChartData>, nodeTag: string, item: TChartData) {
        return true;
    }
    
    transformChartData(nodeTag: string, item: TTransformedData): Array<TChartData> {
        return [{
            ...item as any,
            Key: abstractTransformingChartsWebsocketWidget.chartKey(nodeTag)
        }] as TChartData[];
    }

    transformSocketData(nodeTag: string, input: TPayload): Array<TTransformedData> {
        const tData = { ...input, Key: abstractTransformingChartsWebsocketWidget.chartKey(nodeTag) } as any;
        return [tData];
    }
}

export = abstractTransformingChartsWebsocketWidget;
