import generalUtils = require("common/generalUtils");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import mountPointUsage = require("models/resources/widgets/mountPointUsage");


class perNodeStorageStats {
    readonly tag: string;
    disconnected = ko.observable<boolean>(true);
    
    hasData = ko.observable<boolean>(false);
    
    mountPoints = ko.observableArray<mountPointUsage>([]);

    constructor(tag: string) {
        this.tag = tag;
    }

    update(data: Raven.Server.Dashboard.Cluster.Notifications.StorageUsagePayload) {
        this.hasData(true);
        
        // doing 'in situ' update for better performance  
        const toDelete = new Set<mountPointUsage>(this.mountPoints());
        for (const incomingItem of data.Items) {
            const existingItem = this.mountPoints().find(x => x.mountPoint() === incomingItem.MountPoint);
            if (existingItem) {
                existingItem.update(incomingItem);
                toDelete.delete(existingItem);
            } else {
                const newItem = new mountPointUsage();
                newItem.update(incomingItem);
                this.mountPoints.push(newItem);
            }
        }

        toDelete.forEach(mountPointToDelete => {
            this.mountPoints.remove(mountPointToDelete);
        });
    }
}

class storageWidget extends websocketBasedWidget<Raven.Server.Dashboard.Cluster.Notifications.StorageUsagePayload> {
    nodeStats = ko.observableArray<perNodeStorageStats>([]);

    sizeFormatter = generalUtils.formatBytesToSize;
    
    previousElementsCount: number = -1;
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStorageStats(node.tag());
            this.nodeStats.push(stats);
        }
    }

    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "StorageUsage";
    }

    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
        
        this.initTooltips();
    }
    
    private initTooltips() {
        $('[data-toggle="tooltip"]', this.container).tooltip();
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.StorageUsagePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(false));
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    protected afterSyncUpdate(updatesCount: number) {
        const newElementsCount = this.nodeStats()
            .map(nodeStats => nodeStats.mountPoints().length)
            .reduce((a, b) => a + b, 0);

        if (this.previousElementsCount !== newElementsCount) {
            this.controller.layout(false, "shift");
            this.initTooltips();
        }
        
        this.previousElementsCount = newElementsCount;
    }

    private withStats(nodeTag: string, action: (stats: perNodeStorageStats) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = storageWidget;
