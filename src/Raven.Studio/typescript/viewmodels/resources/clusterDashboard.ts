import viewModelBase = require("viewmodels/viewModelBase");
import cpuUsageWidget = require("viewmodels/resources/widgets/cpuUsageWidget");
import licenseWidget = require("viewmodels/resources/widgets/licenseWidget");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import widget = require("./widgets/widget");

class clusterDashboard extends viewModelBase {
    
    private packery: PackeryStatic;
    
    liveClient = ko.observable<clusterDashboardWebSocketClient>();
    
    widgets = ko.observableArray([new cpuUsageWidget(["A"])]);
    
    private initPackery() {
        this.packery = new Packery( ".masonry-grid", {
            itemSelector: ".cluster-dashboard-item",
            percentPosition: true,
            columnWidth: ".grid-sizer",
            gutter: ".gutter-sizer",
            transitionDuration: '0',
        });
    }

    deactivate() {
        super.deactivate();

        if (this.liveClient()) {
            this.liveClient().dispose();
        }
    }
    
    private enableLiveView() {
        const nodeTag = "A";
        const wsClient = new clusterDashboardWebSocketClient(nodeTag, d => this.onData(nodeTag, d)); //TODO: can't hardcode node tag!
        //TODO: watch failure!
        wsClient.connectToWebSocketTask.done(() => {
            for (const widget of this.widgets()) {
                this.configureWidget(wsClient, widget);
            }
        });
        this.liveClient(wsClient);
    }
    
    private configureWidget(ws: clusterDashboardWebSocketClient, widget: widget<unknown>) {
        ws.sendCommand({
            Command: "watch",
            Config: widget.getConfiguration(),
            Id: widget.id,
            Type: widget.getType()
        });
    }

    private onData(nodeTag: string, msg: Raven.Server.ClusterDashboard.WidgetMessage) {
        const targetWidget = this.widgets().find(x => x.id === msg.Id);
        targetWidget.onData(nodeTag, msg.Data as any);
    }
    
    compositionComplete() {
        super.compositionComplete();
        
        this.initPackery();
        
        this.enableLiveView();
        
        document.querySelectorAll('.cluster-dashboard-item').forEach(item => {
            const draggie = new Draggabilly( item );
            this.packery.bindDraggabillyEvents( draggie );
        });
        
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

        document.querySelectorAll('.toggle-fullscreen').forEach(item => {
            item.addEventListener('click', event => {
                let targetId = (event.target as any).closest('.toggle-fullscreen').dataset.targetid;
                targetId = targetId.replace("#", '');
                document.getElementById(targetId).classList.toggle('fullscreen');
                pckry.layout();
                setTimeout(function () {
                    pckry.layout();
                }, 250);

            });
        });

        document.querySelectorAll('.remove-item').forEach(item => {
            item.addEventListener('click', event => {
                let targetId = (event.target as any).closest('.remove-item').dataset.targetid;
                targetId = targetId.replace("#", '');
                document.getElementById(targetId).remove();;
                pckry.layout();
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
}


export = clusterDashboard;
