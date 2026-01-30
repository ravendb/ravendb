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

interface RemoteAttachmentErrorInfo {
    Date: string;
    Error: string;
    Identifier: string;
    Hash: string;
    Ids: string[];
}

interface RemoteAttachmentErrorsDetails {
    Errors: RemoteAttachmentErrorInfo[];
}

class remoteAttachmentErrorDetails extends abstractAlertDetails {

    view = require("views/common/notificationCenter/detailViewer/alerts/remoteAttachmentErrorDetails.html");

    currentDetails = ko.observable<RemoteAttachmentErrorInfo>();

    tableItems: RemoteAttachmentErrorInfo[] = [];
    private gridController = ko.observable<virtualGridController<RemoteAttachmentErrorInfo>>();
    private columnPreview = new columnPreviewPlugin<RemoteAttachmentErrorInfo>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        this.tableItems = (this.alert.details() as RemoteAttachmentErrorsDetails).Errors;

        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {

            const previewColumn = new actionColumn<RemoteAttachmentErrorInfo>(
                grid, item => this.showDetails(item), "Preview", `<i class="icon-preview"></i>`, "70px",
            {
                title: () => 'Show item preview'
            });
            const dateColumn = new textColumn<RemoteAttachmentErrorInfo>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Date", "15%", {
                sortable: x => x.Date
            });
            const idsColumn = new textColumn<RemoteAttachmentErrorInfo>(grid, x => x.Ids.join(', '), "Ids", "10%", {
                sortable: x => x.Ids.join(', ')
            });
            const identifierColumn = new textColumn<RemoteAttachmentErrorInfo>(grid, x => x.Identifier, "Identifier", "15%", {
                sortable: x => x.Identifier
            });
            const errorColumn = new textColumn<RemoteAttachmentErrorInfo>(grid, x => x.Error, "Error", "45%", {
                sortable: x => x.Error
            });

            return [previewColumn, dateColumn, idsColumn, identifierColumn, errorColumn];
        });

        this.columnPreview.install(".remoteAttachmentErrorDetails", ".js-remote-attachment-error-details-tooltip",
            (details: RemoteAttachmentErrorInfo,
             column: textColumn<RemoteAttachmentErrorInfo>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {

                    if (column.header === "Date") {
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

    private showDetails(item: RemoteAttachmentErrorInfo) {
        this.currentDetails(item);
    }

    copyToClipboard(item: RemoteAttachmentErrorInfo) {
        copyToClipboard.copy(item.Error, "Error has been copied to clipboard", document.getElementById("js-remote-attachment-error-details"));
    }

    private fetcher(): JQueryPromise<pagedResult<RemoteAttachmentErrorInfo>> {
        return $.Deferred<pagedResult<RemoteAttachmentErrorInfo>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertReason() === "Attachments_RemoteAttachmentErroredIdentifier";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new remoteAttachmentErrorDetails(alert, center));
    }
}

export = remoteAttachmentErrorDetails;
