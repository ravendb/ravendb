import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import alert = require("common/notifications/models/alert");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import moment = require("moment");
import ActionDetails = Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails.ActionDetails;

class conflictExceededDetails extends abstractAlertDetails {

    view = require("views/common/notificationCenter/detailViewer/alerts/conflictExceededDetails.html");

    tableItems: ActionDetails[] = [];
    private gridController = ko.observable<virtualGridController<ActionDetails>>();
    private columnPreview = new columnPreviewPlugin<ActionDetails>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const details = alert.details() as Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails;
        this.tableItems = details.Details;

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<ActionDetails>(grid, x => x.Id, "Id", "20%", {
                    sortable: "string"
                }),
                new textColumn<ActionDetails>(grid, x => x.Reason, "Exceeded Value", "25%", {
                    sortable: "string"
                }),
                new textColumn<ActionDetails>(grid, x => x.Deleted, "Deleted Revisions", "25%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<ActionDetails>(grid, x => generalUtils.formatUtcDateAsLocal(x.Time), "Date", "20%", {
                    sortable: x => x.Time
                }),
            ];
        });

        this.columnPreview.install(".conflictExceededDetails", ".js-conflict-exceeded-tooltip",
            (details: ActionDetails, column: textColumn<ActionDetails>, e: JQuery.TriggeredEvent,
                onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (column.header === "Date") {
                    onValue(moment.utc(details.Time), details.Time);
                } else if (value !== undefined) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });
    }

    private fetcher(): JQueryPromise<pagedResult<ActionDetails>> {
        return $.Deferred<pagedResult<ActionDetails>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "ConflictRevisionsExceeded";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new conflictExceededDetails(alert, center));
    }
}

export = conflictExceededDetails;
