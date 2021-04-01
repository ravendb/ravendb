import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractTableWidget = require("viewmodels/resources/widgets/abstractTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import databaseDiskUsage = require("models/resources/widgets/databaseDiskUsage");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");

class databaseStorageWidget extends abstractTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload, perNodeStatItems<databaseDiskUsage>, databaseDiskUsage> {
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseStorageUsage";
    }

    noDatabases = ko.observable<boolean>(true);

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<databaseDiskUsage>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload): databaseDiskUsage[] {
        return data.Items.map(x => new databaseDiskUsage(nodeTag, x));
    }

    onData(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload) {
        super.onData(nodeTag, data);

        //TODO: what if all items are not relevant on node?
        //TODO: check offline items! 
        this.noDatabases(!data.Items);
    }

    protected customizeGrid() {
        this.gridController().customRowClassProvider(item => item.even ? ["even"] : []);
    }

    protected prepareColumns(containerWidth: number, results: pagedResult<databaseDiskUsage>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<databaseDiskUsage>(grid, x => x.hideDatabaseName ? "" : x.database, "Database", "35%"),
            new nodeTagColumn<databaseDiskUsage>(grid, item => this.prepareUrl(item)),
            new textColumn<databaseDiskUsage>(grid, x => x.size, "Data", "15%"),
            new textColumn<databaseDiskUsage>(grid, x => x.tempBuffersSize, "Temp", "15%"),
            new textColumn<databaseDiskUsage>(grid, x => x.total, "Total", "15%"),
        ];
    }

    protected prepareGridData(): JQueryPromise<pagedResult<databaseDiskUsage>> {
        let items: databaseDiskUsage[] = [];

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

    protected prepareUrl(item: databaseDiskUsage): { url: string; openInNewTab: boolean } {
        const database = item.database;
        const nodeTag = item.nodeTag;
        // TODO: what if given database is not relevant on given node?
        const currentNodeTag = this.clusterManager.localNodeTag();
        const targetNode = this.clusterManager.getClusterNodeByTag(nodeTag)

        const link = database === "<System>" ? appUrl.forSystemStorageReport() : appUrl.forStatusStorageReport(database);
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

export = databaseStorageWidget;
