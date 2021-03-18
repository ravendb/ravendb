import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import generalUtils = require("common/generalUtils");
import lineChart = require("models/resources/clusterDashboard/lineChart");

type MemoryWidgetPayload = Raven.Server.ClusterDashboard.Widgets.MemoryUsagePayload;

class perNodeMemoryStats {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);

    hasData = ko.observable<boolean>(false);
    
    availableMemory = ko.observable<number>();
    lowMemorySeverity = ko.observable<Sparrow.LowMemory.LowMemorySeverity>();
    physicalMemory = ko.observable<number>();
    workingSet = ko.observable<number>();
    managedAllocations = ko.observable<number>();
    dirtyMemory = ko.observable<number>();
    encryptionBuffersInUse = ko.observable<number>(); //TODO: consider hiding if encryption not used on server?
    encryptionBuffersPool = ko.observable<number>();
    memoryMapped = ko.observable<number>();
    unmanagedAllocations = ko.observable<number>();
    availableMemoryForProcessing = ko.observable<number>();
    systemCommitLimit = ko.observable<number>();
    
    workingSetFormatted: KnockoutComputed<[string, string]>;
    machineMemoryUsage: KnockoutComputed<string>;
    machineMemoryUsagePercentage: KnockoutComputed<string>;
    lowMemoryTitle: KnockoutComputed<string>;
    
    sizeFormatter = generalUtils.formatBytesToSize;
    
    constructor(tag: string) {
        this.tag = tag;
        
        this.workingSetFormatted = this.valueAndUnitFormatter(this.workingSet);
        
        this.machineMemoryUsage = ko.pureComputed(() => {
            const physical = this.physicalMemory();
            const available = this.availableMemory();
            
            const used = physical - available;
            const usedFormatted = generalUtils.formatBytesToSize(used).split(" ");
            const totalFormatted = generalUtils.formatBytesToSize(physical).split(" ");
            
            if (usedFormatted[1] === totalFormatted[1]) { // same units - avoid repeating ourselves
                return usedFormatted[0] + " / " + totalFormatted[0] + " " + totalFormatted[1];
            } else {
                return usedFormatted[0] + " " + usedFormatted[1] + " / " + totalFormatted[0] + " " + totalFormatted[1];
            }
        })
        
        this.machineMemoryUsagePercentage = ko.pureComputed(() => {
            const physical = this.physicalMemory();
            const available = this.availableMemory();
            
            if (!physical) {
                return "n/a";
            }
            
            return Math.round(100.0 * (physical - available) / physical) + '%';
        });
        
        this.lowMemoryTitle = ko.pureComputed(() => {
            const lowMem = this.lowMemorySeverity();
            if (lowMem === "ExtremelyLow") {
                return "Extremely Low Memory Mode";
            } else if (lowMem === "Low") {
                return "Low Memory Mode";
            }
            
            return null; 
        })
    }
    
    update(data: MemoryWidgetPayload) {
        this.hasData(true);
        this.availableMemory(data.AvailableMemory);
        this.lowMemorySeverity(data.LowMemorySeverity);
        this.physicalMemory(data.PhysicalMemory);
        this.workingSet(data.WorkingSet);
        
        this.managedAllocations(data.ManagedAllocations);
        this.dirtyMemory(data.DirtyMemory);
        this.encryptionBuffersInUse(data.EncryptionBuffersInUse);
        this.encryptionBuffersPool(data.EncryptionBuffersPool);
        this.memoryMapped(data.MemoryMapped);
        this.unmanagedAllocations(data.UnmanagedAllocations);
        this.availableMemoryForProcessing(data.AvailableMemoryForProcessing);
        this.systemCommitLimit(data.SystemCommitLimit);
    }
    
    valueAndUnitFormatter(value: KnockoutObservable<number>): KnockoutComputed<[string, string]> {
        return ko.pureComputed(() => {
            if (this.disconnected() || !this.hasData()) {
                return ["Connecting...", "-"];
            }
            
            const formatted = generalUtils.formatBytesToSize(value());
            return formatted.split(" ", 2) as [string, string];
        });
    }
}

interface memoryUsageState {
    showProcessDetails: boolean;
    showMachineDetails: boolean;
}

class memoryUsageWidget extends websocketBasedWidget<MemoryWidgetPayload, void, memoryUsageState> {

    showProcessDetails = ko.observable<boolean>(false);
    showMachineDetails = ko.observable<boolean>(false);
    
    ravenChart: lineChart;
    serverChart: lineChart;
    
    nodeStats = ko.observableArray<perNodeMemoryStats>([]);
    
    constructor(controller: clusterDashboard, state: memoryUsageState = undefined) {
        super(controller, undefined, state);
        
        _.bindAll(this, "toggleProcessDetails", "toggleMachineDetails");

        for (const node of this.controller.nodes()) {
            const stats = new perNodeMemoryStats(node.tag());
            this.nodeStats.push(stats);
        }
    }
    
    getType(): Raven.Server.ClusterDashboard.WidgetType {
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
            fillData: true
        });
        const serverChartContainer = this.container.querySelector(".machine-line-chart");
        this.serverChart = new lineChart(serverChartContainer, {
            grid: true, 
            fillData: true
        });
    }

    onData(nodeTag: string, data: MemoryWidgetPayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));
        
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
        
        //TODO: send info to line chart!

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        //TODO: send info to line chart!
        
        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }
    
    private withStats(nodeTag: string, action: (stats: perNodeMemoryStats) => void) {
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
