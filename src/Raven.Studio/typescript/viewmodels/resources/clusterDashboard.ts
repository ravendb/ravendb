import app = require("durandal/app");
import viewModelBase = require("viewmodels/viewModelBase");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import widget = require("viewmodels/resources/widgets/widget");
import addWidgetModal = require("viewmodels/resources/addWidgetModal");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import memoryUsageWidget = require("viewmodels/resources/widgets/memoryUsageWidget");
import cpuUsageWidget = require("viewmodels/resources/widgets/cpuUsageWidget");
import licenseWidget = require("viewmodels/resources/widgets/licenseWidget");
import storageWidget = require("viewmodels/resources/widgets/storageWidget");
import clusterNode = require("models/database/cluster/clusterNode");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import indexingWidget = require("viewmodels/resources/widgets/indexingWidget");
import trafficWidget = require("viewmodels/resources/widgets/trafficWidget");
import databaseIndexingWidget = require("viewmodels/resources/widgets/databaseIndexingWidget");
import databaseStorageWidget = require("viewmodels/resources/widgets/databaseStorageWidget");
import databaseTrafficWidget = require("viewmodels/resources/widgets/databaseTrafficWidget");
import EVENTS = require("common/constants/events");

interface savedWidget {
    type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType;
    left: number;
    fullscreen: boolean;
    config: any;
    state: any;
}

class clusterDashboard extends viewModelBase {
    
    private packery: PackeryStatic;
    initialized = ko.observable<boolean>(false);
    
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
            initialLayout: false,
            columnWidth: ".grid-sizer",
            gutter: ".gutter-sizer",
            transitionDuration: '0',
        });
    }
    
    private afterLayoutInitialized() {
        const throttledLayoutSave = _.debounce(() => {
            const packeryWidth = this.packery.packer.width;
            const layout: savedWidget[] = this.widgets().map(x => {
                return {
                    type: x.getType(),
                    left: this.packery.getItem(x.container).rect.x / packeryWidth,
                    fullscreen: x.fullscreen(),
                    config: x.getConfiguration(),
                    state: x.getState()
                }
            });

            localStorage.setObject("cluster-dashboard-layout", layout);
        }, 5_000);

        this.packery.on("layoutComplete", throttledLayoutSave);

        this.initialized(true);
    }
    
    createPostboxSubscriptions(): Array<KnockoutSubscription> {
        return [
            ko.postbox.subscribe(EVENTS.Menu.Resized, () => {
                this.packery.layout();
            })
        ];
    }

    compositionComplete() {
        super.compositionComplete();
        
        if (this.nodes().length) {
            this.initDashboard();
        } else {
            // wait for cluster boostrap
            const awaitClusterInit = this.nodes.subscribe(nodes => {
                if (nodes.length && !this.initialized()) {
                    this.initDashboard();
                    awaitClusterInit.dispose();
                }
            });
        }
    }
    
    private initDashboard(): JQueryPromise<void> {
        this.initPackery();

        this.enableLiveView();

        const savedLayout: savedWidget[] = localStorage.getObject("cluster-dashboard-layout");
        if (savedLayout) {
            const widgets = savedLayout.map(item => this.spawnWidget(item.type, item.fullscreen, item.config, item.state));
            return $.when(...widgets.map(x => x.composeTask))
                .done(() => {
                    this.packery._resetLayout();

                    const packeryWidth = this.packery.packer.width;

                    for (let i = 0; i < savedLayout.length; i++) {
                        const savedItem = savedLayout[i];
                        const widget = widgets[i];
                        const packeryItem = this.packery.getItem(widget.container);
                        packeryItem.rect.x = savedItem.left * packeryWidth;
                    }

                    this.packery.shiftLayout();

                    this.afterLayoutInitialized();
                });
        } else {
            this.addWidget(new cpuUsageWidget(this));
            this.addWidget(new memoryUsageWidget(this));
            this.addWidget(new licenseWidget(this));
            this.addWidget(new storageWidget(this));
            this.addWidget(new trafficWidget(this));
            this.addWidget(new indexingWidget(this));
            this.addWidget(new databaseIndexingWidget(this));
            this.addWidget(new databaseTrafficWidget(this));
            this.addWidget(new databaseStorageWidget(this));
            
            return $.when(...this.widgets().map(x => x.composeTask))
                .done(() => {
                    this.afterLayoutInitialized();
                });
        }
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
            if (widget instanceof websocketBasedWidget) {
                widget.onClientConnected(ws);
            }
        }
    }

    private onWebSocketDisconnected(ws: clusterDashboardWebSocketClient) {
        for (const widget of this.widgets()) {
            if (widget instanceof websocketBasedWidget) {
                widget.onClientDisconnected(ws);
            }
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
    
    getConnectedLiveClients() {
        return this.liveClients().filter(x => x.isConnected());
    }
    
    getConnectedCurrentNodeLiveClient() {
        return this.getConnectedLiveClients().find(x => x.nodeTag === this.currentServerNodeTag);
    }
    
    onWidgetAdded(widget: widget<any, any>) {
        this.packery.appended([widget.container]);

        const draggie = new Draggabilly(widget.container, {
            handle: ".cluster-dashboard-item-header"
        });
        this.packery.bindDraggabillyEvents(draggie);
    }
 
    private onData(nodeTag: string, msg: Raven.Server.Dashboard.Cluster.WidgetMessage) {
        const targetWidget = this.widgets().find(x => x.id === msg.Id);
        // target widget might be in removal state but 'unwatch' wasn't delivered yet.
        if (targetWidget) {
            if (targetWidget instanceof websocketBasedWidget) {
                targetWidget.onData(nodeTag, msg.Data);
            } else {
                console.error("Tried to deliver message to widget which doesn't support messages. Id = " + msg.Id);
            }
        }
    }
    
    addWidgetModal() {
        const addWidgetView = new addWidgetModal(type => this.spawnWidget(type));
        app.showBootstrapDialog(addWidgetView);
    }
    
    spawnWidget(type: Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType, fullscreen: boolean = false, config: any = undefined, state: any = undefined) {
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
            case "StorageUsage":
                widget = new storageWidget(this);
                break;
            case "Indexing":
                widget = new indexingWidget(this);
                break;
            case "Traffic":
                widget = new trafficWidget(this);
                break;
            case "DatabaseIndexing":
                widget = new databaseIndexingWidget(this);
                break;
            case "DatabaseStorageUsage":
                widget = new databaseStorageWidget(this);
                break;
            case "DatabaseTraffic":
                widget = new databaseTrafficWidget(this);
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
        
        return widget;
    }
}

export = clusterDashboard;
