import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import genUtils from "common/generalUtils";

class queueSinkErrorDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/queueSinkErrorDetails.html");

    tableItems: Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo[] = [];
    private gridController = ko.observable<virtualGridController<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorsDetails;
        this.tableItems = details.Errors;
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
           
            const dateColumn = new textColumn<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>(
                grid, x => genUtils.formatUtcDateAsLocal(x.Date), "Date", "30%", {
                    sortable: x => x.Date
                });
            const errorColumn = new textColumn<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>(
                grid, x => x.Error, "Error", "69%");

            return [dateColumn, errorColumn];
        });
        
        
        this.columnPreview.install(".queueSinkErrorDetails", ".js-queue-sink-error-details-tooltip",
            (details: Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo,
             column: textColumn<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>> {
        return $.Deferred<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) 
            && (notification.alertType() === "QueueSink_ConsumeError" || notification.alertType() === "QueueSink_ScriptError");
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new queueSinkErrorDetails(alert, center));
    }
}

export = queueSinkErrorDetails;
