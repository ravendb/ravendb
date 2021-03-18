import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class perNodeCpuStats {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);
    
    hasData = ko.observable<boolean>(false);

    machineCpuUsage = ko.observable<number>(0);
    processCpuUsage = ko.observable<number>(0);

    numberOfCores = ko.observable<number>();
    utilizedCores = ko.observable<number>();
    
    coresInfo: KnockoutComputed<string>;
    processCpuUsageFormatted: KnockoutComputed<string>;
    machineCpuUsageFormatted: KnockoutComputed<string>;
    
    constructor(tag: string) {
        this.tag = tag;
        
        this.coresInfo = ko.pureComputed(() => {
            if (!this.hasData()) {
                return "-/- Cores";
            }
            return this.utilizedCores() + "/" + this.numberOfCores() + " Cores";
        });
        
        this.processCpuUsageFormatted = ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "Connecting...";
            }
            return this.processCpuUsage() + "%";
        });

        this.machineCpuUsageFormatted = ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return "Connecting...";
            }
            return this.machineCpuUsage() + "%";
        });
    }
    
    update(data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        this.hasData(true);
        
        this.machineCpuUsage(data.MachineCpuUsage);
        this.processCpuUsage(data.ProcessCpuUsage);

        this.numberOfCores(data.NumberOfCores);
        this.utilizedCores(data.UtilizedCores);
    }
}

class cpuUsageWidget extends websocketBasedWidget<Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload> {
   
    ravenChart: lineChart;
    serverChart: lineChart;
    
    nodeStats = ko.observableArray<perNodeCpuStats>([]);
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeCpuStats(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.ClusterDashboard.WidgetType {
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
    
    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));

        const date = moment.utc(data.Time).toDate();
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

        //TODO: send info to line chart!

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        //TODO: send info to line chart!

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: perNodeCpuStats) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = cpuUsageWidget;
