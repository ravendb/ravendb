import generalUtils = require("common/generalUtils");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");

class mountPointStats {
    freeSpace = ko.observable<number>();
    isLowSpace = ko.observable<boolean>(false);
    mountPoint = ko.observable<string>();
    ravenSize = ko.observable<number>();
    totalCapacity = ko.observable<number>();
    volumeLabel = ko.observable<string>();

    usedSpace: KnockoutComputed<number>;
    usedSpacePercentage: KnockoutComputed<number>;
    ravendbToUsedSpacePercentage: KnockoutComputed<number>;
    
    mountPointLabel: KnockoutComputed<string>;
    
    constructor() {
        this.mountPointLabel = ko.pureComputed(() => {
            let mountPoint = this.mountPoint();
            const mountPointLabel = this.volumeLabel();
            if (mountPointLabel) {
                mountPoint += ` (${mountPointLabel})`;
            }

            return mountPoint;
        });

        this.usedSpace = ko.pureComputed(() => {
            const total = this.totalCapacity();
            const free = this.freeSpace();
            return total - free;
        });

        this.ravendbToUsedSpacePercentage = ko.pureComputed(() => {
            const totalUsed = this.usedSpace();
            const documentsUsed = this.ravenSize();

            if (!totalUsed) {
                return 0;
            }

            return documentsUsed *  100.0 / totalUsed;
        });

        this.usedSpacePercentage = ko.pureComputed(() => {
            const total = this.totalCapacity();
            const used = this.usedSpace();

            if (!total) {
                return 0;
            }

            return used * 100.0 / total;
        });
    }
    
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
    disconnected = ko.observable<boolean>(true);
    
    hasData = ko.observable<boolean>(false);
    
    mountPoints = ko.observableArray<mountPointStats>([]);

    constructor(tag: string) {
        this.tag = tag;
    }

    update(data: Raven.Server.ClusterDashboard.Widgets.StoragePayload) {
        this.hasData(true);
        
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

class storageWidget extends websocketBasedWidget<Raven.Server.ClusterDashboard.Widgets.StoragePayload> {
    nodeStats = ko.observableArray<perNodeStorageStats>([]);

    sizeFormatter = generalUtils.formatBytesToSize;
    
    previousElementsCount: number = -1;
    
    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStorageStats(node.tag());
            this.nodeStats.push(stats);
        }
        
        //TODO: remove me!
        setTimeout(() => {
            this.onData("A", {
                Items: [
                    {
                        MountPoint: "/data",
                        VolumeLabel: null,
                        TotalCapacity: 249414283264,
                        FreeSpace: 54122274816,
                        IsLowSpace: false,
                        RavenSize: 33816576
                    }
                ]
            })
        }, 5000);

        //TODO: remove me!
        setTimeout(() => {
            this.onData("B", {
                Items: [
                    {
                        MountPoint: "/data",
                        VolumeLabel: null,
                        TotalCapacity: 249414283264,
                        FreeSpace: 54122274816,
                        IsLowSpace: false,
                        RavenSize: 33816576
                    },
                    {
                        MountPoint: "/dataB",
                        VolumeLabel: null,
                        TotalCapacity: 249414283264,
                        FreeSpace: 54122274816,
                        IsLowSpace: false,
                        RavenSize: 33816576
                    }
                ]
            })
        }, 10_000);
        
    }

    getType(): Raven.Server.ClusterDashboard.WidgetType {
        return "Storage";
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

    onData(nodeTag: string, data: Raven.Server.ClusterDashboard.Widgets.StoragePayload) {
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
