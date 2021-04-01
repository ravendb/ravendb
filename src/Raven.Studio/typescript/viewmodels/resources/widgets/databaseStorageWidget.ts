import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import databaseDiskUsage = require("models/resources/widgets/databaseDiskUsage");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");

class databaseStorageWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseStorageUsagePayload, perNodeStatItems<databaseDiskUsage>, databaseDiskUsage> {
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseStorageUsage";
    }

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

    protected generateLocalLink(database: string): string {
        return database === "<System>" ? appUrl.forSystemStorageReport() : appUrl.forStatusStorageReport(database);
    }
}

export = databaseStorageWidget;
