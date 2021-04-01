import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

interface statsBase<TItem> {
    disconnected: KnockoutObservable<boolean>;
    tag: string;
    items: TItem[];
}

abstract class abstractTableWidget<TRaw, TStats extends statsBase<TTableItem>, TTableItem> extends websocketBasedWidget<TRaw> {
    protected gridController = ko.observable<virtualGridController<TTableItem>>();
    protected clusterManager = clusterTopologyManager.default;

    spinners = {
        loading: ko.observable<boolean>(true)
    }

    nodeStats = ko.observableArray<TStats>([]);

    onData(nodeTag: string, data: TRaw) {
        this.scheduleSyncUpdate(() => {
            this.spinners.loading(false);
            this.withStats(nodeTag, x => {
                x.items = this.mapItems(nodeTag, data);
            });
        });
    }
    
    protected abstract mapItems(nodeTag: string, data: TRaw): TTableItem[];
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        grid.headerVisible(true);
        
        this.customizeGrid();

        grid.init((s, t) => this.prepareGridData(), (containerWidth, results) => this.prepareColumns(containerWidth, results));

        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
    }
    
    protected customizeGrid() {
        // override to customize
    }
    
    protected abstract prepareGridData(): JQueryPromise<pagedResult<TTableItem>>;
    
    protected abstract prepareColumns(containerWidth:number, results: pagedResult<TTableItem>): virtualColumn[];

    protected afterSyncUpdate(updatesCount: number) {
        this.gridController().reset(false);
    }

    protected afterComponentResized() {
        this.gridController().reset(true, true);
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: TStats) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }

    newDatabase() {
        console.log("TODO"); //TODO:
    }
}

export = abstractTableWidget;
