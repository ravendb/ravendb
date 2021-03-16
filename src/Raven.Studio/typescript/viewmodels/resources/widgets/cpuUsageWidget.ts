import widget = require("viewmodels/resources/widgets/widget");
import lineChart = require("models/resources/clusterDashboard/lineChart");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class perNodeCpuStats {
    readonly tag: string;
    loading = ko.observable<boolean>(true);
    disconnected = ko.observable<boolean>(false);

    machineCpuUsage = ko.observable<number>();
    processCpuUsage = ko.observable<number>();

    numberOfCores = ko.observable<number>();
    utilizedCores = ko.observable<number>();
    
    coresInfo: KnockoutComputed<string>;
    
    constructor(tag: string) {
        this.tag = tag;
        
        this.coresInfo = ko.pureComputed(() => {
            if (!this.utilizedCores() && !this.numberOfCores()) {
                return "";
            }
            return this.utilizedCores() + "/" + this.numberOfCores() + " Cores";
        });
    }
    
    update(data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        this.machineCpuUsage(data.MachineCpuUsage);
        this.processCpuUsage(data.ProcessCpuUsage);

        this.numberOfCores(data.NumberOfCores);
        this.utilizedCores(data.UtilizedCores);
    }
}

class cpuUsageWidget extends widget<Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload> {
   
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

        this.fullscreen.subscribe(() => {
            //TODO: throttle + wait for animation to complete?
            setTimeout(() => {
                this.ravenChart.onResize();
                this.serverChart.onResize();
            }, 500);
        });
    }
    
    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.CpuUsagePayload) {
        this.withStats(nodeTag, x => x.update(data));

        const date = moment.utc(data.Time).toDate();
        const key = "node-" + nodeTag.toLocaleLowerCase();

        this.ravenChart.onData(date, [{
            key,
            value: data.ProcessCpuUsage
        }]);

        this.serverChart.onData(date, [{
            key,
            value: data.MachineCpuUsage
        }])
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        //TODO: send info to line chart!

        this.withStats(ws.nodeTag, x => {
            x.loading(true);
            x.disconnected(false);
        });
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
