import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import genUtils from "common/generalUtils";

interface WarningItem {
    source: string;
    collection: string;
    count: number;
}

class blockingTombstonesDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/blockingTombstonesDetails.html");

    tableItems: WarningItem[] = [];
    private gridController = ko.observable<virtualGridController<WarningItem>>();
    private columnPreview = new columnPreviewPlugin<WarningItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const warning = this.alert.details() as any;
        const detailsList = warning.BlockingTombstones as Raven.Server.NotificationCenter.TombstoneNotifications.BlockingTombstoneDetails[];
        
        this.tableItems = detailsList.map(x => ({
            source: x.Source,
            collection: x.Collection,
            count: x.NumberOfTombstones
        }));
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const sourceColumn = new textColumn<WarningItem>(grid, x => x.source, "Source", "35%", {
                sortable: x => x.source
            });
            const collectionColumn = new textColumn<WarningItem>(grid, x => x.collection, "Collection", "40%", {
                sortable: x => x.collection
            });
            const countColumn = new textColumn<WarningItem>(grid, x => x.count, "Tombstones count", "25%");

            return [sourceColumn, collectionColumn, countColumn];
        });
        
        
        this.columnPreview.install(".blockingTombstonesDetails", ".js-blocking-tombstones-detials-tooltip",
            (details: WarningItem,
             column: textColumn<WarningItem>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
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
