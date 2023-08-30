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

type conflictExceededDetailsItemDto = {
    Id: string;
    Reason: string;
    Deleted: number;
    Time: string;
}

class conflictExceededDetails extends abstractAlertDetails {

    view = require("views/common/notificationCenter/detailViewer/alerts/conflictExceededDetails.html");

    tableItems: conflictExceededDetailsItemDto[] = [];
    private gridController = ko.observable<virtualGridController<conflictExceededDetailsItemDto>>();
    private columnPreview = new columnPreviewPlugin<conflictExceededDetailsItemDto>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        this.tableItems = this.mapItems(alert.details() as Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails);

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<conflictExceededDetailsItemDto>(grid, x => x.Id, "Id", "20%", {
                    sortable: "string"
                }),
                new textColumn<conflictExceededDetailsItemDto>(grid, x => x.Reason, "Exceeded Value", "25%", {
                    sortable: "string"
                }),
                new textColumn<conflictExceededDetailsItemDto>(grid, x => x.Deleted, "Deleted Revisions", "25%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<conflictExceededDetailsItemDto>(grid, x => generalUtils.formatUtcDateAsLocal(x.Time), "Date", "20%", {
                    sortable: x => x.Time
                }),
            ];
        });

        this.columnPreview.install(".conflictExceededDetails", ".js-conflict-exceeded-tooltip",
            (details: conflictExceededDetailsItemDto, column: textColumn<conflictExceededDetailsItemDto>, e: JQueryEventObject,
                onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (column.header === "Date") {
                    onValue(moment.utc(details.Time), details.Time);
                } else if (!_.isUndefined(value)) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });
    }

    private fetcher(): JQueryPromise<pagedResult<conflictExceededDetailsItemDto>> {
        return $.Deferred<pagedResult<conflictExceededDetailsItemDto>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    private mapItems(details: Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails): conflictExceededDetailsItemDto[] {
        return details.Details.map(item => {
            return {
                Id: item.Id,
                Deleted: item.Deleted,
                Reason: item.Reason,
                Time: item.Time
            }
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "Revisions";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new conflictExceededDetails(alert, center));
    }
}

export = conflictExceededDetails;
