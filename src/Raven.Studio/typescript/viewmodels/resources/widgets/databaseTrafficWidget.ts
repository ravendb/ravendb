import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import trafficWatchItem = require("models/resources/widgets/trafficWatchItem");
import generalUtils = require("common/generalUtils");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import widget = require("viewmodels/resources/widgets/widget");

class databaseTrafficWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload, 
    perNodeStatItems<trafficWatchItem>, trafficWatchItem> {
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseTraffic";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<trafficWatchItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload): trafficWatchItem[] {
        return data.Items.map(x => new trafficWatchItem(nodeTag, x));
    }

    protected prepareColumns(containerWidth: number, results: pagedResult<trafficWatchItem>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<trafficWatchItem>(grid, x => x.hideDatabaseName ? "" : x.database, "Database", "35%"),
            new nodeTagColumn<trafficWatchItem>(grid, item => this.prepareUrl(item)),
            new textColumn<trafficWatchItem>(grid, x => widget.formatNumber(x.requestsPerSecond), "Requests/s", "15%"),
            new textColumn<trafficWatchItem>(grid, x => widget.formatNumber(x.writesPerSecond), "Writes/s", "15%"),
            new textColumn<trafficWatchItem>(grid, x => generalUtils.formatBytesToSize(x.dataWritesPerSecond), "Data written/s", "15%"),
        ];
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forTrafficWatch(database);
    }
}

export = databaseTrafficWidget;
