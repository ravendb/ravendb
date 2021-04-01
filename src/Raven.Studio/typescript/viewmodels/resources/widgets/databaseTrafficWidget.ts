import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractTableWidget = require("viewmodels/resources/widgets/abstractTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import trafficWatchItem = require("models/resources/widgets/trafficWatchItem");
import databaseTrafficWatch = require("models/resources/widgets/databaseTrafficWatch");
import generalUtils = require("common/generalUtils");

class databaseTrafficWidget extends abstractTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload, databaseTrafficWatch, trafficWatchItem> {
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseTraffic";
    }

    noDatabases = ko.observable<boolean>(true);

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new databaseTrafficWatch(node.tag());
            this.nodeStats.push(stats);
        }
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload) {
        super.onData(nodeTag, data);

        //TODO: what if all items are not relevant on node?
        //TODO: check offline items! 
        this.noDatabases(!data.Items);
    }

    protected customizeGrid() {
        this.gridController().customRowClassProvider(item => item.even ? ["even"] : []);
    }

    protected prepareColumns(containerWidth: number, results: pagedResult<trafficWatchItem>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<trafficWatchItem>(grid, x => x.hideDatabaseName ? "" : x.database, "Database", "35%"),
            new nodeTagColumn<trafficWatchItem>(grid, item => this.prepareUrl(item)),
            new textColumn<trafficWatchItem>(grid, x => x.requestsPerSecond.toLocaleString(), "Requests/s", "15%"),
            new textColumn<trafficWatchItem>(grid, x => x.writesPerSecond.toLocaleString(), "Writes/s", "15%"),
            new textColumn<trafficWatchItem>(grid, x => generalUtils.formatBytesToSize(x.dataWritesPerSecond), "Data written/s", "15%"),
        ];
    }

    protected prepareGridData(): JQueryPromise<pagedResult<trafficWatchItem>> {
        let items: trafficWatchItem[] = [];

        this.nodeStats().forEach(nodeStat => {
            items.push(...nodeStat.items);
        });

        //TODO: alpha numeric sort!
        items = _.sortBy(items, ["database", "nodeTag"]);

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

        return $.when({
            totalResultCount: items.length,
            items
        });
    }

    protected prepareUrl(item: trafficWatchItem): { url: string; openInNewTab: boolean } {
        const database = item.database;
        const nodeTag = item.nodeTag;
        // TODO: what if given database is not relevant on given node?
        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag)

        const link = database === "<System>" ? appUrl.forTrafficWatch() : appUrl.forTrafficWatch(database);
        if (currentNodeTag === nodeTag) {
            return {
                url: link,
                openInNewTab: false
            }
        } else {
            return {
                url: appUrl.toExternalUrl(targetNode.serverUrl(), link),
                openInNewTab: true
            }
        }
    }
}

export = databaseTrafficWidget;
