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
import welcomeWidget = require("viewmodels/resources/widgets/welcomeWidget");
import databaseIndexingWidget = require("viewmodels/resources/widgets/databaseIndexingWidget");
import databaseStorageWidget = require("viewmodels/resources/widgets/databaseStorageWidget");
import databaseTrafficWidget = require("viewmodels/resources/widgets/databaseTrafficWidget");
import EVENTS = require("common/constants/events");
import storageKeyProvider = require("common/storage/storageKeyProvider");

interface savedWidgetsLayout {
    widgets: savedWidget[];
    columns: number;
}

interface savedWidget {
    type: widgetType;
    columnIndex: number;
    fullscreen: boolean;
    config: any;
    state: any;
}

class clusterDashboard extends viewModelBase {

    static localStorageName = storageKeyProvider.storageKeyFor("clusterDashboardLayout");
    
    private packery: PackeryStatic;
    initialized = ko.observable<boolean>(false);
    readonly currentServerNodeTag: string;
    
    widgets = ko.observableArray<widget<any>>([]);
    
    nodes: KnockoutComputed<clusterNode[]>;
    bootstrapped: KnockoutComputed<boolean>;
    
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
        
        this.bootstrapped = ko.pureComputed(() => !!this.nodes().length);
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
            const layout = this.widgets().map(x => {
                const packeryItem = this.packery.getItem(x.container);
                return {
                    left: packeryItem.rect.x / packeryWidth,
                    top: packeryItem.rect.y,
                    widget: x
                }
            });
            
            const sortedLayout = layout.sort((a, b) => a.top === b.top ? a.left - b.left : a.top - b.top);
            
            const columnsCount = this.getNumberOfColumnsInPackeryLayout();
            
            const widgetsLayout: savedWidgetsLayout = {
                widgets: sortedLayout.map(x => ({
                    type: x.widget.getType(),
                    fullscreen: x.widget.fullscreen(),
                    config: x.widget.getConfiguration(),
                    state: x.widget.getState(),
                    columnIndex: clusterDashboard.getColumnIndex(x.left, columnsCount)
                })),
                columns: columnsCount
            };
            localStorage.setObject(clusterDashboard.localStorageName, widgetsLayout);
        }, 5_000);

        this.packery.on("layoutComplete", throttledLayoutSave);

        this.initialized(true);
    }
    
    private static getColumnIndex(leftPositionPercentage: number, totalColumns: number): number {
        return Math.round(leftPositionPercentage * totalColumns);
    }
    
    private getNumberOfColumnsInPackeryLayout() {
        const gridSizer = $(".cluster-dashboard-container .grid-sizer").innerWidth();
        return Math.round(this.packery.packer.width / gridSizer);
    }
    
    attached() {
        super.attached();

        $("#page-host").css("overflow-y", "scroll");
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
                if (nodes.length) {
                    this.initialized(false);
                    this.widgets([]);
                    awaitClusterInit.dispose();
                    
                    setTimeout(() => {
                        this.initDashboard();    
                    }, 500);
                }
            });
            
            // but in meantime we want to show welcome widget only to avoid empty screen on newly started server
            // (since it isn't bootstrapped by default)
            this.clusterIsNotBootstrapped();
        }
    }
    
    private clusterIsNotBootstrapped() {
        this.initPackery();
        const welcome = this.spawnWidget("Welcome", true);
        this.addWidget(welcome);
        
        welcome.composeTask.done(() => {
            this.onWidgetAdded(welcome);
            this.initialized(true);
        });
    }
    
    private initDashboard(): JQueryPromise<void> {
        this.initPackery();
        
        this.widgets([]);

        this.enableLiveView();

        const savedWidgetsLayout: savedWidgetsLayout = localStorage.getObject(clusterDashboard.localStorageName);
        if (savedWidgetsLayout) {
            const currentColumnsCount = this.getNumberOfColumnsInPackeryLayout();
            const sameColumnsCount = savedWidgetsLayout.columns === currentColumnsCount;
            
            const savedLayout = savedWidgetsLayout.widgets;
            const widgets = savedLayout.map(item => this.spawnWidget(item.type, item.fullscreen, item.config, item.state));
            
            widgets.forEach(w => this.addWidget(w));
            
            return $.when(...widgets.map(x => x.composeTask))
                .done(() => {
                    widgets.forEach(w => this.onWidgetAdded(w));
                    
                    if (sameColumnsCount) {
                        // saved and current columns count is the same 
                        // try to restore layout with regard to positions within columns 
                        // and try to respect items orders inside each column 
                        this.packery._resetLayout();
                        const gutterWidth = this.packery.gutter;
                        const itemWidth = this.packery.columnWidth;

                        for (let i = 0; i < savedLayout.length; i++) {
                            const savedItem = savedLayout[i];
                            const widget = widgets[i];
                            const packeryItem = this.packery.getItem(widget.container);
                            packeryItem.rect.x = savedItem.columnIndex * (itemWidth + gutterWidth); 
                        }

                        this.packery.shiftLayout();

                        this.afterLayoutInitialized();
                    } else {
                        // looks like columns count changed - let call fresh layout
                        // but it should maintain item's order
                        // that's all we can do
                        this.packery.layout();
                        this.afterLayoutInitialized();
                    }
                });
        } else {
            const welcome = new welcomeWidget(this);
            welcome.fullscreen(true);
            this.addWidget(welcome);
            this.addWidget(new cpuUsageWidget(this));
            this.addWidget(new memoryUsageWidget(this));
            this.addWidget(new licenseWidget(this));
            this.addWidget(new storageWidget(this));
            this.addWidget(new trafficWidget(this));
            this.addWidget(new indexingWidget(this));
            this.addWidget(new databaseIndexingWidget(this));
            this.addWidget(new databaseTrafficWidget(this));
            this.addWidget(new databaseStorageWidget(this));
            
            const initialWidgets = this.widgets();
            
            return $.when(...this.widgets().map(x => x.composeTask))
                .done(() => {
                    initialWidgets.forEach(w => this.onWidgetAdded(w));
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
    
    detached() {
        super.detached();
        
        $("#page-host").css("overflow-y", "");
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
        const existingWidgetTypes = _.uniq(this.widgets().map(x => x.getType()));
        const addWidgetView = new addWidgetModal(existingWidgetTypes, type => {
            const newWidget = this.spawnWidget(type);
            this.addWidget(newWidget);
            
            newWidget.composeTask.done(() => {
                this.onWidgetAdded(newWidget);
            })
        });
        app.showBootstrapDialog(addWidgetView);
    }
    
    spawnWidget(type: widgetType, fullscreen: boolean = false, config: any = undefined, state: any = undefined) {
        let widget: widget<any>;
        
        switch (type) {
            case "Welcome":
                widget = new welcomeWidget(this);
                break;
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
        
        return widget;
    }
}

export = clusterDashboard;
