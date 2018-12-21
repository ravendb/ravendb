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

class slowSqlDetails extends abstractPerformanceHintDetails {

    currentDetails = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>();
    
    tableItems = [] as Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo[];
    private gridController = ko.observable<virtualGridController<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        this.tableItems = (this.hint.details() as Raven.Server.NotificationCenter.Notifications.Details.SlowSqlDetails).Statements;

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            
            const previewColumn = new actionColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>(
                grid, item => this.showDetails(item), "Preview", `<i class="icon-preview"></i>`, "70px",
            {
                title: () => 'Show item preview'
            });
            
            return [
                    previewColumn,
                    new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>(grid, x => x.Duration, "Duration (ms)", "15%", {
                        sortable: "number"
                    }),
                    new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                        sortable: x => x.Date
                    }),
                    new textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>(grid, x => x.Statement, "Statement", "50%", {
                        sortable: "string"
                    })
                ];
        });
        
        this.columnPreview.install(".slowSqlDetails", ".js-slow-sql-details-tooltip",
            (details: Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo,
             column: textColumn<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>,
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
    
    private showDetails(item: Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo) {
        this.currentDetails(item);
    }
    
    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>> {
        return $.Deferred<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.SlowSqlStatementInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "SqlEtl_SlowSql";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new slowSqlDetails(hint, center));
    }
}

export = slowSqlDetails;
