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
import moment = require("moment");

interface EtlAlertTableItem extends Partial<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo> {
    TimeSeriesName?: string;
}

class etlTransformOrLoadErrorDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/etlTransformOrLoadErrorDetails.html");

    readonly isWarningDetails: boolean;

    currentDetails = ko.observable<EtlAlertTableItem>();
    
    tableItems: EtlAlertTableItem[] = [];
    private gridController = ko.observable<virtualGridController<EtlAlertTableItem>>();
    private columnPreview = new columnPreviewPlugin<EtlAlertTableItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        this.isWarningDetails = this.alert.alertReason() === "Etl_Warning";

        if (this.isWarningDetails) {
            const details = this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.EtlWarningDetails;
            this.tableItems = [{
                DocumentId: details.DocumentId,
                TimeSeriesName: details.TimeSeriesName
            }];
        } else {
            this.tableItems = (this.alert.details() as Raven.Server.NotificationCenter.Notifications.Details.EtlErrorsDetails)
                .Errors
                .slice()
                .reverse();
        }
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const documentIdColumn = new textColumn<EtlAlertTableItem>(grid, x => x.DocumentId || ' - ', "Document ID", this.isWarningDetails ? "50%" : "20%", {
                sortable: x => x.DocumentId,
                customComparator: generalUtils.sortAlphaNumeric
            });

            if (this.isWarningDetails) {
                const timeSeriesNameColumn = new textColumn<EtlAlertTableItem>(grid, x => x.TimeSeriesName || ' - ', "Time Series Name", "50%", {
                    sortable: x => x.TimeSeriesName,
                    customComparator: generalUtils.sortAlphaNumeric
                });

                return [documentIdColumn, timeSeriesNameColumn];
            }
            
            const previewColumn = new actionColumn<EtlAlertTableItem>(
                grid, item => this.showDetails(item), "Preview", `<i class="icon-preview"></i>`, "70px",
            {
                title: () => 'Show item preview'
            });
            const dateColumn = new textColumn<EtlAlertTableItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "20%", {
                sortable: x => x.Date
            });
            const errorColumn = new textColumn<EtlAlertTableItem>(grid, x => x.Error, "Error", "50%", {
                sortable: x => x.Error
            });
            
            return this.alert.alertReason() === "Etl_LoadError" ?
                [previewColumn, dateColumn, errorColumn, documentIdColumn] :
                [previewColumn, documentIdColumn, dateColumn, errorColumn];
            });

        this.columnPreview.install(".etlErrorDetails", ".js-etl-error-details-tooltip",
            (details: EtlAlertTableItem,
             column: textColumn<EtlAlertTableItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    
                    if (column.header === "Date" && details.Date) {
                        onValue(moment.utc(details.Date), details.Date);       
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });
    }
    
    private showDetails(item: EtlAlertTableItem) {
        if (item.Error) {
            this.currentDetails(item);
        }
    }
    
    copyToClipboard(item: EtlAlertTableItem) {
        if (item.Error) {
            copyToClipboard.copy(item.Error, "Error has been copied to clipboard", document.getElementById("js-etl-error-details"));
        }
    }

    private fetcher(): JQueryPromise<pagedResult<EtlAlertTableItem>> {
        return $.Deferred<pagedResult<EtlAlertTableItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) &&
            (notification.alertReason() == "Etl_LoadError" ||
                notification.alertReason() == "Etl_TransformationError" ||
                notification.alertReason() == "Etl_Warning");
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new etlTransformOrLoadErrorDetails(alert, center));
    }
}

export = etlTransformOrLoadErrorDetails;
