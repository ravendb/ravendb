import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import genUtils from "common/generalUtils";
import ServerLimitInfo = Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails.ServerLimitInfo;

class serverLimitsDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/serverLimitsDetails.html");

    tableItems: Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails.ServerLimitInfo[] = [];
    private gridController = ko.observable<virtualGridController<ServerLimitInfo>>();
    private columnPreview = new columnPreviewPlugin<ServerLimitInfo>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails;
        this.tableItems = details.Limits;
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const nameColumn = new textColumn<ServerLimitInfo>(grid, x => x.Name, "Name", "25%", {
                sortable: x => x.Name
            });
            const limitColumn = new textColumn<ServerLimitInfo>(grid, x => x.Limit, "Limit", "20%", {
                sortable: x => x.Limit
            });
            const dateColumn = new textColumn<ServerLimitInfo>(grid, x => genUtils.formatUtcDateAsLocal(x.Date), "Date", "30%", {
                sortable: x => x.Date
            });
            const valueColumn = new textColumn<ServerLimitInfo>(grid, x => x.Value, "Value", "12%");
            const maxColumn = new textColumn<ServerLimitInfo>(grid, x => x.Max, "Max", "12%");

            return [dateColumn, nameColumn, limitColumn, valueColumn, maxColumn];
        });
        
        
        this.columnPreview.install(".serverLimitsDetails", ".js-server-limits-details-tooltip",
            (details: ServerLimitInfo,
             column: textColumn<ServerLimitInfo>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<ServerLimitInfo>> {
        return $.Deferred<pagedResult<ServerLimitInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "ServerLimits";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new serverLimitsDetails(alert, center));
    }
}

export = serverLimitsDetails;
