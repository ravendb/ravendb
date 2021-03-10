import viewModelBase = require("viewmodels/viewModelBase");
import cpuUsageWidget = require("viewmodels/resources/widgets/cpuUsageWidget");
import licenseWidget = require("viewmodels/resources/widgets/licenseWidget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import widget = require("viewmodels/resources/widgets/widget");
import memoryUsageWidget = require("viewmodels/resources/widgets/memoryUsageWidget");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");

class clusterDashboard extends viewModelBase {
    
    private packery: PackeryStatic;
    
    private readonly currentServerNodeTag: string;
    
    readonly nodeTags: string[];
    
    widgets = ko.observableArray([]);  
    
    liveClients = ko.observableArray<clusterDashboardWebSocketClient>([]);
    
    constructor() {
        super();
        
        const topologyManager = clusterTopologyManager.default;

        this.currentServerNodeTag = topologyManager.localNodeTag();
        const nodes = topologyManager.topology().nodes();

        this.nodeTags = nodes.map(x => x.tag());
    }
    
    private initPackery() {
        this.packery = new Packery( ".masonry-grid", {
            itemSelector: ".cluster-dashboard-item",
            percentPosition: true,
            columnWidth: ".grid-sizer",
            gutter: ".gutter-sizer",
            transitionDuration: '0',
        });
    }

    compositionComplete() {
        super.compositionComplete();

        this.initPackery();

        this.enableLiveView();

        document.querySelectorAll('.cluster-dashboard-item').forEach(item => {
            const draggie = new Draggabilly( item );
            this.packery.bindDraggabillyEvents( draggie );
        });
        
        setTimeout(() => {
            this.addWidget(new cpuUsageWidget(this.nodeTags, this));
            this.addWidget(new memoryUsageWidget(this.nodeTags, this));
            this.addWidget(new licenseWidget(this.nodeTags, this));
        }, 5000);
        
        /* TODO: 
        $(function () {
            $('[data-toggle="tooltip"]').tooltip({
                container: 'body'
            })
        })

        document.querySelectorAll('.property-control').forEach(item => {
            item.addEventListener('click', event => {
                let targetId = (event.target as any).closest('.property-control').dataset.targetid;
                targetId = targetId.replace("#", '');
                document.getElementById(targetId).classList.toggle('property-collapse');
                pckry.layout();
                setTimeout(function () {
                    pckry.layout();
                }, 250);

            });
        });


        try {
            document.getElementById('nodeB').addEventListener('click', event => {
                document.getElementById('topology').classList.toggle('selected');
            });

            const collapseElements = document.querySelectorAll(".collapse");

            $('.collapse').on('hidden.bs.collapse', function () {
                pckry.layout();
            })

            $('.collapse').on('shown.bs.collapse', function () {
                pckry.layout();
            })
        } catch (e) {
            console.error(e);
        }
*/

    }
    
    layout(withDelay: boolean = true) {
        if (withDelay) {
            setTimeout(() => {
                this.packery.layout();
            }, 300);
        } else {
            this.packery.layout();
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
    
    deleteWidget(widget: widget<any>) {
        this.packery.remove(widget.container);
        this.packery.shiftLayout();
        
        this.widgets.remove(widget);

        for (const liveClient of this.liveClients()) {
            //TODO: check what if not connected?
            this.unsubscribeFromWidgetUpdates(liveClient, widget);
        }
    }
    
    addWidget(widget: widget<any>) {
        this.widgets.push(widget);
        
        const element = this.findWidgetElement(widget);
        
        this.packery.appended([element]);

        for (const ws of this.liveClients()) {
            this.subscribeForWidgetUpdates(ws, widget);
        }

        const draggie = new Draggabilly( element );
        this.packery.bindDraggabillyEvents( draggie );
    }
    
    //TODO: can we avoid this method?
    private findWidgetElement(widget: widget<any>): Element {
        const element = document.querySelector(`[data-widget-id='${widget.id}']`);
        
        if (!element) {
            throw new Error("Unable to find widget with id = " + widget.id);
        }
        
        return element;
    }
    
    private enableLiveView() {
        for (const nodeTag of this.nodeTags) {
            const client = new clusterDashboardWebSocketClient(nodeTag, d => this.onData(nodeTag, d));
            this.liveClients.push(client);
            
            client.connectToWebSocketTask.done(() => this.configureClient(client));
        }
    }
    
    private configureClient(ws: clusterDashboardWebSocketClient) {
        for (const widget of this.widgets()) {
            this.subscribeForWidgetUpdates(ws, widget);
        }
    }
    
    private subscribeForWidgetUpdates(ws: clusterDashboardWebSocketClient, widget: widget<any>) {
        if (widget.supportedOnNode(ws.nodeTag, this.currentServerNodeTag)) {
            //TODO: add check if not connected then wait?
            ws.sendCommand({
                Command: "watch",
                Config: widget.getConfiguration(),
                Id: widget.id,
                Type: widget.getType()
            });
        }
    }
    
    private unsubscribeFromWidgetUpdates(ws: clusterDashboardWebSocketClient, widget: widget<any>) {
        if (widget.supportedOnNode(ws.nodeTag, this.currentServerNodeTag)) {
            ws.sendCommand({
                Command: "unwatch",
                Id: widget.id
            } as Raven.Server.ClusterDashboard.WidgetRequest);
        }
    }
    
    private onData(nodeTag: string, msg: Raven.Server.ClusterDashboard.WidgetMessage) {
        const targetWidget = this.widgets().find(x => x.id === msg.Id);
        // target widget might be in removal state but 'unwatch' wasn't delivered yet.
        if (targetWidget) {
            targetWidget.onData(nodeTag, msg.Data as any);
        }
    }
    
}


export = clusterDashboard;
