import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");

class slowWriteDetails extends abstractPerformanceHintDetails {

    tableItems = [] as Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo[];
    private gridController = ko.observable<virtualGridController<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        // extract values - ignore keys
        this.tableItems = _.map((this.hint.details() as Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails).Writes, v => v);

        // newest first
        this.tableItems.reverse();
    }
    
    private formatSpeed(info: Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo) {
        return info.DataWrittenInMb.toFixed(2) + " MB in " + info.DurationInSec.toFixed(2) + " sec. (" + info.SpeedInMbPerSec.toFixed(2) + "MB/s)"
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () =>
            [
                new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>(grid, x => x.Path, "Path", "45%", {
                    sortable: "string"
                }),
                new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                    sortable: x => x.Date
                }),
                new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>(grid,
                    x => this.formatSpeed(x), "Speed", "20%", {
                        sortable: x => x.SpeedInMbPerSec
                    })
            ]);
        
        this.columnPreview.install(".slowWriteDetails", ".js-slow-write-details-tooltip",
            (details: Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo,
             column: textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    if (column.header === "Date") {
                        onValue(moment.utc(details.Date), details.Date);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(value);
                        }
                    }
                }
            });
    }
    
    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>> {
        return $.Deferred<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.SlowWritesDetails.SlowWriteInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "SlowIO";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new slowWriteDetails(hint, center));
    }
}

export = slowWriteDetails;
