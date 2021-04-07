import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

import lineChart = require("models/resources/clusterDashboard/lineChart");
import memoryUsage = require("models/resources/widgets/memoryUsage");


interface memoryUsageState {
    showProcessDetails: boolean;
    showMachineDetails: boolean;
}

class memoryUsageWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.MemoryUsagePayload, void, memoryUsageState> {

    private readonly throttledShowHistory: (date: Date) => void;
    
    showProcessDetails = ko.observable<boolean>(false);
    showMachineDetails = ko.observable<boolean>(false);
    
    ravenChart: lineChart;
    serverChart: lineChart;
    
    nodeStats = ko.observableArray<memoryUsage>([]);
    
    constructor(controller: clusterDashboard, state: memoryUsageState = undefined) {
        super(controller, undefined, state);
        
        _.bindAll(this, "toggleProcessDetails", "toggleMachineDetails");

        for (const node of this.controller.nodes()) {
            const stats = new memoryUsage(node.tag());
            this.nodeStats.push(stats);
        }

        this.throttledShowHistory = _.throttle((d: Date) => this.showNodesHistory(d), 100);
    }
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "MemoryUsage";
    }

    getState(): memoryUsageState {
        return {
            showMachineDetails: this.showMachineDetails(),
            showProcessDetails: this.showProcessDetails()
        }
    }

    restoreState(state: memoryUsageState) {
        this.showProcessDetails(state.showProcessDetails);
        this.showMachineDetails(state.showMachineDetails);
    }

    attached(view: Element, container: HTMLElement) {
        super.attached(view, container);
        
        this.initTooltip();
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
        const ravenChartContainer = this.container.querySelector(".ravendb-line-chart");
        this.ravenChart = new lineChart(ravenChartContainer, {
            grid: true,
            fillData: true,
            tooltipProvider: date => memoryUsageWidget.tooltipContent(date),
            onMouseMove: date => this.onMouseMove(date)
        });
        const serverChartContainer = this.container.querySelector(".machine-line-chart");
        this.serverChart = new lineChart(serverChartContainer, {
            grid: true, 
            fillData: true,
            tooltipProvider: date => memoryUsageWidget.tooltipContent(date),
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

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.MemoryUsagePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.onData(data)));
        
        const date = moment.utc(data.Time).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();
        
        this.scheduleSyncUpdate(() => {
            this.ravenChart.onData(date, [{
                key,
                value: data.WorkingSet
            }]);

            this.serverChart.onData(date, [{
                key,
                value: data.PhysicalMemory - data.AvailableMemory
            }]);
        });
    }

    protected afterSyncUpdate(updatesCount: number) {
        if (updatesCount) {
            this.serverChart.draw();
            this.ravenChart.draw();
        }
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
    
    private withStats(nodeTag: string, action: (stats: memoryUsage) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
    
    toggleProcessDetails() {
        this.showProcessDetails.toggle();

        this.controller.layout(true, "shift");
    }

    toggleMachineDetails() {
        this.showMachineDetails.toggle();

        this.controller.layout(true, "shift");
    }
}

export = memoryUsageWidget;
