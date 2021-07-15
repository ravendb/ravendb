import clusterDashboard = require("viewmodels/resources/clusterDashboard");
import nodeTagColumn = require("widgets/virtualGrid/columns/nodeTagColumn");
import abstractDatabaseAndNodeAwareTableWidget = require("viewmodels/resources/widgets/abstractDatabaseAndNodeAwareTableWidget");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import iconsPlusTextColumn = require("widgets/virtualGrid/columns/iconsPlusTextColumn");
import appUrl = require("common/appUrl");
import perNodeStatItems = require("models/resources/widgets/perNodeStatItems");
import databaseOverviewItem = require("models/resources/widgets/databaseOverviewItem");

class databaseOverviewWidget extends abstractDatabaseAndNodeAwareTableWidget<Raven.Server.Dashboard.Cluster.Notifications.DatabaseOverviewPayload,
    perNodeStatItems<databaseOverviewItem>, databaseOverviewItem> {
    
    getType(): Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType {
        return "DatabaseOverview";
    }

    constructor(controller: clusterDashboard) {
        super(controller);

        for (const node of this.controller.nodes()) {
            const stats = new perNodeStatItems<databaseOverviewItem>(node.tag());
            this.nodeStats.push(stats);
        }
    }

    protected createNoDataItem(nodeTag: string, databaseName: string): databaseOverviewItem {
        return databaseOverviewItem.noData(nodeTag, databaseName);
    }

    protected mapItems(nodeTag: string, data: Raven.Server.Dashboard.Cluster.Notifications.DatabaseOverviewPayload): databaseOverviewItem[] {
        return data.Items.map(x => new databaseOverviewItem(nodeTag, x));
    }
    
    protected manageItems(items: databaseOverviewItem[]): void {
        if (items.length) {
            const firstItem = items[0];
            let firstDbName = firstItem.database;
            let commonItem = databaseOverviewItem.commonData(firstItem.nodeTag, firstItem.database, firstItem.documents, firstItem.indexes, firstItem.ongoingTasks, firstItem.backupInfo);
            items.unshift(commonItem);
            
            if (items.length > 2) {
                for (let i = 2; i < items.length - 1; i++) {
                    const item = items[i];
                    let dbName = item.database;
                    if (dbName != firstDbName) {
                        commonItem = databaseOverviewItem.commonData(item.nodeTag, item.database, item.documents, item.indexes, item.ongoingTasks, item.backupInfo);
                        items.splice(i, 0, commonItem);
                        firstDbName = dbName;
                    }
                }
            }
        }
    }

    protected applyPerDatabaseStripes(items: databaseOverviewItem[]) {
        for (let i = 0; i < items.length; i++) {
            const item = items[i];
            
            if (item.isCommonItem) {
                item.even = true;
            } else {
                item.even = false;
                item.hideDatabaseName = true;
            }
        }
    }

    protected prepareColumns(containerWidth: number, results: pagedResult<databaseOverviewItem>): virtualColumn[] {
        const grid = this.gridController();
        return [
            new textColumn<databaseOverviewItem>(grid, x => x.hideDatabaseName ? "" : x.database, "Database", "20%"),
            
            new nodeTagColumn<databaseOverviewItem>(grid, item => this.prepareUrl(item), "Documents"),

            new textColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? x.documents : "", "Documents", "10%"),
            
            new iconsPlusTextColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? "" : x.alertsDataForHtml(), "Alerts", "10%"),
            
            new iconsPlusTextColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? x.indexes.toLocaleString() : x.erroredIndexesDataForHtml(), "Indexes", "10%"),
            
            new iconsPlusTextColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? "" : x.indexingErrorsDataForHtml(), "Indexing Errors", "10%"),

            new textColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? x.ongoingTasks.toLocaleString() : "", "Ongoing Tasks", "10%"),

            new iconsPlusTextColumn<databaseOverviewItem>(grid, x => x.isCommonItem ? x.backupDataForHtml() : "", "Backups", "10%"),
        ];
    }

    protected generateLocalLink(database: string): string {
        return appUrl.forDocuments(null, database);
    }
}

export = databaseOverviewWidget;
