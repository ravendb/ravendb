import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import genUtils from "common/generalUtils";

interface TableItem {
    indexName: string;
}

class cpuCreditsBalanceDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/cpuCreditsBalanceDetails.html");

    tableItems: TableItem[] = [];
    private gridController = ko.observable<virtualGridController<TableItem>>();
    private columnPreview = new columnPreviewPlugin<TableItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.CpuCreditsExhaustionWarning;
        this.tableItems = details.IndexNames.map(x => ({ indexName: x }));
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () =>
            [
                new textColumn<TableItem>(grid, x => x.indexName, "Index Name", "95%")
            ]);
        
        
        this.columnPreview.install(".cpuCreditsBalanceDetails", ".js-cpu-credits-balance-details-tooltip",
            (details: TableItem,
             column: textColumn<TableItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<TableItem>> {
        return $.Deferred<pagedResult<TableItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) 
            && notification.alertType() === "Throttling_CpuCreditsBalance";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new cpuCreditsBalanceDetails(alert, center));
    }
}

export = cpuCreditsBalanceDetails;
