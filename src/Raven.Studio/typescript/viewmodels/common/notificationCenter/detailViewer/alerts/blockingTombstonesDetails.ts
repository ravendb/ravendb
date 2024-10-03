import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import genUtils from "common/generalUtils";
import hyperlinkColumn from "widgets/virtualGrid/columns/hyperlinkColumn";
import appUrl from "common/appUrl";
import activeDatabaseTracker from "common/shell/activeDatabaseTracker";

interface WarningItem {
    source: string;
    collection: string;
    count: number;
    blockerType: Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType;
    blockerTaskId: number;
    size: number;
}

class blockingTombstonesDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/blockingTombstonesDetails.html");

    tableItems: WarningItem[] = [];
    private gridController = ko.observable<virtualGridController<WarningItem>>();
    private columnPreview = new columnPreviewPlugin<WarningItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const warning = this.alert.details() as any;
        const detailsList = warning.BlockingTombstones as Raven.Server.NotificationCenter.BlockingTombstoneDetails[];
        
        this.tableItems = detailsList.map(x => ({
            blockerType: x.BlockerType,
            blockerTaskId: x.BlockerTaskId,
            source: x.Source,
            collection: x.Collection,
            count: x.NumberOfTombstones,
            size: x.SizeOfTombstonesInBytes
        }));
    }
    
    private static getBrokerTypeDescription(type: Raven.Server.Documents.ITombstoneAware.TombstoneDeletionBlockerType) {
        switch (type) {
            case "RavenEtl":
                return "RavenDB ETL";
            case "ExternalReplication":
                return "External Replication";
            case "SqlEtl":
                return "SQL ETL";
            case "SnowflakeEtl":
                return "Snowflake ETL";
            case "OlapEtl":
                return "OLAP ETL";
            case "ElasticSearchEtl":
                return "Elasticsearch ETL";
            case "PullReplicationAsHub":
                return "Replication Hub";
            case "PullReplicationAsSink":
                return "Replication Sink";
            case "QueueEtl":
                return "Queue ETL";
            default:
                return type;
        }
    }
    
    private static formatSource(item: WarningItem) {
        return blockingTombstonesDetails.getBrokerTypeDescription(item.blockerType) + ' "' + item.source + '"';
    }
    
    private static linkToSource(item: WarningItem) {
        const currentDatabase = activeDatabaseTracker.default.database();
        
        switch (item.blockerType) {
            case "Index":
                return appUrl.forEditIndex(item.source, currentDatabase);
            case "Backup":
                return appUrl.forEditPeriodicBackupTask(currentDatabase, "Backups", false, item.blockerTaskId);
            case "ElasticSearchEtl":
                return appUrl.forEditElasticSearchEtl(currentDatabase, item.blockerTaskId);
            case "ExternalReplication":
                return appUrl.forEditExternalReplication(currentDatabase, item.blockerTaskId);
            case "OlapEtl":
                return appUrl.forEditOlapEtl(currentDatabase, item.blockerTaskId);
            case "PullReplicationAsHub":
                return appUrl.forEditReplicationHub(currentDatabase, item.blockerTaskId);
            case "PullReplicationAsSink":
                return appUrl.forEditReplicationSink(currentDatabase, item.blockerTaskId);
            case "SqlEtl":
                return appUrl.forEditSqlEtl(currentDatabase, item.blockerTaskId);
            case "SnowflakeEtl":
                return appUrl.forEditSnowflakeEtl(currentDatabase, item.blockerTaskId);
            case "RavenEtl":
                return appUrl.forEditRavenEtl(currentDatabase, item.blockerTaskId);
            default:
                // we fall back to ongoing task list
                return appUrl.forOngoingTasks(currentDatabase);
        }
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const sourceColumn = new hyperlinkColumn<WarningItem>(grid, x => blockingTombstonesDetails.formatSource(x), x => blockingTombstonesDetails.linkToSource(x), "Blockage source", "35%", {
                sortable: x => x.blockerType + "_" + x.source,
                handler: () => this.close()
            });
            const collectionColumn = new textColumn<WarningItem>(grid, x => x.collection, "Collection", "20%", {
                sortable: x => x.collection
            });
            const countColumn = new textColumn<WarningItem>(grid, x => x.count, "Tombstones count", "20%");
            const sizeColumn = new textColumn<WarningItem>(grid, x => x.size, "Size", "20%", {
                sortable: x => x.size,
                transformValue: genUtils.formatBytesToSize
            });

            return [sourceColumn, collectionColumn, countColumn, sizeColumn];
        });
        
        
        this.columnPreview.install(".blockingTombstonesDetails", ".js-blocking-tombstones-details-tooltip",
            (details: WarningItem,
             column: textColumn<WarningItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<WarningItem>> {
        return $.Deferred<pagedResult<WarningItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "BlockingTombstones";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new blockingTombstonesDetails(alert, center));
    }
}

export = blockingTombstonesDetails;
