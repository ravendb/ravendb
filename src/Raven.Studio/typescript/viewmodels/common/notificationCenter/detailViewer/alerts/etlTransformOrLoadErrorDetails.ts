import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import copyToClipboard = require("common/copyToClipboard");
import generalUtils = require("common/generalUtils");

class etlTransformOrLoadErrorDetails extends abstractAlertDetails {

    currentDetails = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>();
    
    tableItems = [] as Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo[];
    private gridController = ko.observable<virtualGridController<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>>();
    private columnPreview = new columnPreviewPlugin<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        this.tableItems = (this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.EtlErrorsDetails).Errors;

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            
            const previewColumn = new actionColumn<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>(
                grid, item => this.showDetails(item), "Preview", `<i class="icon-preview"></i>`, "70px",
            {
                title: () => 'Show item preview'
            });
            const dateColumn = new textColumn<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                sortable: x => x.Date
            });
            const errorColumn = new textColumn<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>(grid, x => x.Error, "Error", "50%", {
                sortable: x => x.Error
            });
            const documentIdColumn = new textColumn<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>(grid, x => x.DocumentId || ' - ', "Document ID", "20%", {
                sortable: x => x.DocumentId
            });
            
            return this.alert.alertType() === "Etl_LoadError" ?
                [previewColumn, dateColumn, errorColumn, documentIdColumn] :
                [previewColumn, documentIdColumn, dateColumn, errorColumn];
            });

        this.columnPreview.install(".etlErrorDetails", ".js-etl-error-details-tooltip",
            (details: Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo,
             column: textColumn<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>,
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
    
    private showDetails(item: Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo) {
        this.currentDetails(item);
    }
    
    copyToClipboard(item: Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo) {
        copyToClipboard.copy(item.Error, "Error has been copied to clipboard", document.getElementById("js-etl-error-details"));
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>> {
        return $.Deferred<pagedResult<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && (notification.alertType() == "Etl_LoadError" || notification.alertType() == "Etl_TransformationError");
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new etlTransformOrLoadErrorDetails(alert, center));
    }
}

export = etlTransformOrLoadErrorDetails;
