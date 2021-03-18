import widget = require("viewmodels/resources/widgets/widget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class mountPointStats {
    freeSpace = ko.observable<number>();
    isLowSpace = ko.observable<boolean>(false);
    mountPoint = ko.observable<string>();
    ravenSize = ko.observable<number>();
    totalCapacity = ko.observable<number>();
    volumeLabel = ko.observable<string>();
    
    update(data: Raven.Server.ClusterDashboard.Widgets.StorageMountPointPayload) {
        this.freeSpace(data.FreeSpace);
        this.isLowSpace(data.IsLowSpace);
        this.mountPoint(data.MountPoint);
        this.ravenSize(data.RavenSize);
        this.totalCapacity(data.TotalCapacity);
        this.volumeLabel(data.VolumeLabel);
    }
}

class perNodeStorageStats {
    readonly tag: string;
    loading = ko.observable<boolean>(true);
    disconnected = ko.observable<boolean>(true);
    
    mountPoints = ko.observableArray<mountPointStats>([]);

    constructor(tag: string) {
        this.tag = tag;
    }

    update(data: Raven.Server.ClusterDashboard.Widgets.StoragePayload) {
        // doing 'in situ' update for better performance  
        const toDelete = new Set<mountPointStats>(this.mountPoints());
        for (const incomingItem of data.Items) {
            const existingItem = this.mountPoints().find(x => x.mountPoint() === incomingItem.MountPoint);
            if (existingItem) {
                existingItem.update(incomingItem);
                toDelete.delete(existingItem);
            } else {
                const newItem = new mountPointStats();
                newItem.update(incomingItem);
                this.mountPoints.push(newItem);
            }
        }

        toDelete.forEach(mountPointToDelete => {
            this.mountPoints.remove(mountPointToDelete);
        });
    }
}

class storageWidget extends widget<Raven.Server.ClusterDashboard.Widgets.StoragePayload> {
    nodeStats = ko.observableArray<perNodeStorageStats>([]);
    
    //TODO: what about <div class="legend"> in html file?

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStorageStats(node.tag());
            this.nodeStats.push(stats);
        }
    }

    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "CpuUsage";
    }

    compositionComplete() {
        super.compositionComplete();
        this.enableSyncUpdates();
    }

    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.StoragePayload) {
        this.scheduleSyncUpdate(() => this.withStats(nodeTag, x => x.update(data)));
    }

    onClientConnected(ws: clusterDashboardWebSocketClient) {
        super.onClientConnected(ws);

        this.withStats(ws.nodeTag, x => {
            x.loading(false);
            x.disconnected(false);
        });
    }

    onClientDisconnected(ws: clusterDashboardWebSocketClient) {
        super.onClientDisconnected(ws);

        this.withStats(ws.nodeTag, x => x.disconnected(true));
    }

    private withStats(nodeTag: string, action: (stats: perNodeStorageStats) => void) {
        const stats = this.nodeStats().find(x => x.tag === nodeTag);
        if (stats) {
            action(stats);
        }
    }
}

export = storageWidget;
