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
import DatabaseUtils = require("components/utils/DatabaseUtils");

class databaseTrafficWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload, 
    perNodeStatItems<trafficWatchItem>, trafficWatchItem> {

    view = require("views/resources/widgets/databaseTrafficWidget.html");
    
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

    protected createNoDataItem(nodeTag: string, databaseName: string): trafficWatchItem {
        return trafficWatchItem.noData(nodeTag, databaseName);
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseTrafficWatchPayload): trafficWatchItem[] {
        return data.Items.map(x => new trafficWatchItem(nodeTag, x));
    }

    protected prepareColumns(): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<trafficWatchItem>(grid, x => x.hideDatabaseName && !grid.sortEnabled() ? "" : DatabaseUtils.default.formatName(x.database), "Database", "30%"),
            new nodeTagColumn<trafficWatchItem>(grid, item => this.prepareUrl(item, "Traffic Watch View")),
            new textColumn<trafficWatchItem>(grid, x => x.requestsPerSecond, "Requests/s", "12%", {
                headerTitle: "Requests made to node per second",
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<trafficWatchItem>(grid, x => x.writesPerSecond, "Writes/s", "12%", {
                headerTitle: "Items written by node per second",
                sortable: "number",
                transformValue: (x: number) => widget.formatNumber(x)
            }),
            new textColumn<trafficWatchItem>(grid, x => x.noData ? -1 : x.dataWritesPerSecond, "Data written/s", "12%", {
                headerTitle: "Bytes written by node per second",
                sortable: "number",
                transformValue: (x: number) => x === -1 ? "-" : generalUtils.formatBytesToSize(x)

            }),
            new textColumn<trafficWatchItem>(grid, x => x.noData ? -1 : Math.round(x.averageDuration) + " ms", "Avg Req Time", "12%", {
                headerTitle: "Average request time",
                sortable: "number",
                transformValue: (x: number) => x === -1 ? "-" : x.toLocaleString()
            }),
        ];
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forTrafficWatch(database);
    }
}

export = databaseTrafficWidget;
