import app = require("durandal/app");
import websocketBasedWidget = require("viewmodels/resources/widgets/websocketBasedWidget");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import clusterDashboardWebSocketClient = require("common/clusterDashboardWebSocketClient");
import clusterTopologyManager = require("common/shell/clusterTopologyManager");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import appUrl = require("common/appUrl");
import generalUtils = require("common/generalUtils");
import createDatabase = require("viewmodels/resources/createDatabase");
import databasesManager = require("common/shell/databasesManager");

interface statsBase<TItem> {
    disconnected: KnockoutObservable<boolean>;
    tag: string;
    items: TItem[];
}

abstract class abstractDatabaseAndNodeAwareTableWidget<TRaw, TStats extends statsBase<TTableItem>, TTableItem extends databaseAndNodeAwareStats> extends websocketBasedWidget<TRaw> {
    protected gridController = ko.observable<virtualGridController<TTableItem>>();
    protected clusterManager = clusterTopologyManager.default;
    protected databaseManager = databasesManager.default;

    noDatabases = ko.pureComputed(() => !this.databaseManager.databases().length);

    spinners = {
        loading: ko.observable<boolean>(true)
    }

    nodeStats = ko.observableArray<TStats>([]);

    onData(nodeTag: string, data: TRaw) {
        this.scheduleSyncUpdate(() => {
            this.spinners.loading(false);
            this.withStats(nodeTag, x => x.items = this.mapItems(nodeTag, data));
        });
    }
    
    protected abstract mapItems(nodeTag: string, data: TRaw): TTableItem[];
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();

        grid.headerVisible(true);

        this.gridController().customRowClassProvider(item => item.even ? ["even"] : []);
        
        grid.init((s, t) => this.prepareGridData(), (containerWidth, results) => this.prepareColumns(containerWidth, results));

        this.enableSyncUpdates();

        for (let ws of this.controller.getConnectedLiveClients()) {
            this.onClientConnected(ws);
        }
    }
    
    protected abstract prepareColumns(containerWidth:number, results: pagedResult<TTableItem>): virtualColumn[];

    protected afterSyncUpdate(updatesCount: number) {
        this.gridController().reset(false);
    }

    protected afterComponentResized() {
        super.afterComponentResized();
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

    protected sortGridData<T extends databaseAndNodeAwareStats>(items: T[]): void {
        items.sort((a, b) => {
            const dbSgn = generalUtils.sortAlphaNumeric(a.database, b.database);
            if (dbSgn) {
                return dbSgn;
            }
            
            return a.nodeTag.localeCompare(b.nodeTag);
        });
    }

    protected applyPerDatabaseStripes(items: databaseAndNodeAwareStats[]) {
        // leave only first database name in group - we don't want to repeat db name
        let currentDbName = "";
        let even = true;

        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            if (item.database === currentDbName) {
                item.hideDatabaseName = true;
            } else {
                currentDbName = item.database;
                even = !even;
            }
            item.even = even;
        }
    }

    protected prepareUrl(item: databaseAndNodeAwareStats): { url: string; openInNewTab: boolean } {
        const database = item.database;
        const nodeTag = item.nodeTag;
        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag)

        const link = this.generateLocalLink(database);
        if (currentNodeTag === nodeTag) {
            return {
                url: link,
                openInNewTab: false
            };
        } else {
            return {
                url: appUrl.toExternalUrl(targetNode.serverUrl(), link),
                openInNewTab: true
            }
        }
    }

    protected prepareGridData(): JQueryPromise<pagedResult<TTableItem>> {
        let items: TTableItem[] = [];

        this.nodeStats().forEach(nodeStat => {
            items.push(...nodeStat.items);
        });

        this.sortGridData(items);

        this.applyPerDatabaseStripes(items);

        return $.when({
            totalResultCount: items.length,
            items
        });
    }
    
    protected abstract generateLocalLink(database: string): string;

    newDatabase() {
        const createDbView = new createDatabase("newDatabase");
        app.showBootstrapDialog(createDbView);
    }
}

export = abstractDatabaseAndNodeAwareTableWidget;
