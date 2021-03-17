import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import cpuUsageWidget = require("viewmodels/resources/widgets/cpuUsageWidget");
import licenseWidget = require("viewmodels/resources/widgets/licenseWidget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import widget = require("viewmodels/resources/widgets/widget");
import memoryUsageWidget = require("viewmodels/resources/widgets/memoryUsageWidget");
import addWidgetModal = require("viewmodels/resources/addWidgetModal");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import debugWidget = require("viewmodels/resources/widgets/debugWidget");
import clusterNode = require("models/database/cluster/clusterNode");

interface savedWidget {
    type: Raven.Server.ClusterDashboard.WidgetType;
    left: string;
    top: string;
    fullscreen: boolean;
    config: any;
    state: any;
}

class clusterDashboard extends viewModelBase {
    
    private packery: PackeryStatic;
    
    readonly currentServerNodeTag: string;
    
    widgets = ko.observableArray<widget<any>>([]);
    
    nodes: KnockoutComputed<clusterNode[]>;
    
    liveClients = ko.observableArray<clusterDashboardWebSocketClient>([]);
    
    constructor() {
        super();
        
        const topologyManager = clusterTopologyManager.default;

        this.currentServerNodeTag = topologyManager.localNodeTag();
        
        this.nodes = ko.pureComputed(() => {
            const topology = topologyManager.topology();
            if (!topology) {
                return [];
            }
            return topologyManager.topology().nodes();
        });
    }
    
    private initPackery() {
        this.packery = new Packery( ".masonry-grid", {
            itemSelector: ".cluster-dashboard-item",
            percentPosition: true,
            columnWidth: ".grid-sizer",
            gutter: ".gutter-sizer",
            transitionDuration: '0',
        });
        
        const throttledLayout = _.debounce(() => {
            const layout: savedWidget[] = this.widgets().map(x => {
                return {
                    type: x.getType(),
                    left: x.container.style.left,
                    top: x.container.style.top,
                    fullscreen: x.fullscreen(),
                    config: x.getConfiguration(),
                    state: x.getState()
                }
            });
            
            localStorage.setObject("cluster-dashboard-layout", layout);
        }, 5_000);
        
        this.packery.on("layoutComplete", throttledLayout);
    }

    compositionComplete() {
        super.compositionComplete();
        //TODO: what if cluster is not bootstraped?

        
        //todo on menu resize
        this.initPackery();

        this.enableLiveView();
        
        const savedLayout: savedWidget[] = localStorage.getObject("cluster-dashboard-layout");
        if (savedLayout) {
            const sortedWidgets = _.sortBy(savedLayout, x => parseFloat(x.top), x => parseFloat(x.left));
            
            for (const item of sortedWidgets) {
                this.spawnWidget(item.type, item.fullscreen, item.config, item.state);
            }
        } else {
            // TODO: this is default list!
            this.addWidget(new cpuUsageWidget(this));
            this.addWidget(new memoryUsageWidget(this));
            this.addWidget(new licenseWidget(this));
            this.addWidget(new debugWidget(this));
        }
    }
    
    printElementsInfo(extra: string) {
        const items = this.packery.getItemElements();
        
        console.log(extra, items.map(x => {
            return $(x).attr("data-widget-id") + " => " + $(x).outerHeight();
        }).join(", "));
    }
    
    private enableLiveView() {
        const nodes = clusterTopologyManager.default.topology().nodes();

        for (const node of nodes) {
            const client: clusterDashboardWebSocketClient =
                new clusterDashboardWebSocketClient(node.tag(), d => this.onData(node.tag(), d), () => this.onWebSocketConnected(client), () => this.onWebSocketDisconnected(client));
            this.liveClients.push(client);
        }
    }

    private onWebSocketConnected(ws: clusterDashboardWebSocketClient) {
        for (const widget of this.widgets()) {
            widget.onClientConnected(ws);
        }
    }

    private onWebSocketDisconnected(ws: clusterDashboardWebSocketClient) {
        for (const widget of this.widgets()) {
            widget.onClientDisconnected(ws);
        }
    }

    deactivate() {
        super.deactivate();

        const clients = this.liveClients();
        
        clients.forEach(client => {
            client?.dispose();
        });

        this.liveClients([]);
    }
    
    deleteWidget(widget: widget<any, any>) {
        this.packery.remove(widget.container);
        this.packery.shiftLayout();
        
        this.widgets.remove(widget);
        
        widget.dispose();
    }
    
    addWidget(widget: widget<any>) {
        this.widgets.push(widget);
    }

    layout(withDelay: boolean = true, mode: "shift" | "full" = "full") {
        const layoutAction = () => {
            mode === "full" ? this.packery.layout() : this.packery.shiftLayout();
        }
        
        if (withDelay) {
            setTimeout(() => {
                layoutAction();
            }, 600);
        } else {
            layoutAction();
        }
    }

    onWidgetReady(widget: widget<any, any>) {
        this.layoutNewWidget(widget);

        for (const ws of this.liveClients()) {
            if (ws.isConnected()) {
                widget.onClientConnected(ws);
            }
        }
    }
    
    private layoutNewWidget(widget: widget<any, any>) {
        this.packery.appended([widget.container]);

        const draggie = new Draggabilly(widget.container);
        this.packery.bindDraggabillyEvents(draggie);
        
        //TODO: with set timeout?
        setTimeout(() => {
            this.layout(false);    
        }, 100);
    }
 
    private onData(nodeTag: string, msg: Raven.Server.ClusterDashboard.WidgetMessage) {
        const targetWidget = this.widgets().find(x => x.id === msg.Id);
        // target widget might be in removal state but 'unwatch' wasn't delivered yet.
        if (targetWidget) {
            targetWidget.onData(nodeTag, msg.Data as any);
        }
    }
    
    addWidgetModal() {
        const addWidgetView = new addWidgetModal(type => this.spawnWidget(type));
        app.showBootstrapDialog(addWidgetView);
    }
    
    spawnWidget(type: Raven.Server.ClusterDashboard.WidgetType, fullscreen: boolean = false, config: any = undefined, state: any = undefined) {
        let widget: widget<any>;
        
        switch (type) {
            case "CpuUsage":
                widget = new cpuUsageWidget(this);
                break;
            case "License":
                widget = new licenseWidget(this);
                break;
            case "MemoryUsage":
                widget = new memoryUsageWidget(this);
                break;
            case "Debug":
                widget = new debugWidget(this);
                break;
            default:
                throw new Error("Unsupported widget type = " + type);
        }
        
        widget.fullscreen(fullscreen);
        
        if (config) {
            widget.restoreConfiguration(config);
        }
        if (state) {
            widget.restoreState(state);
        }
        
        this.addWidget(widget);
    }
}


export = clusterDashboard;
