import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import moment = require("moment");

type hugeDocumentsDetailsItemDto = Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentInfo

class hugeDocumentsDetails extends abstractPerformanceHintDetails {

    view = require("views/common/notificationCenter/detailViewer/performanceHint/hugeDocumentsDetails.html");

    tableItems: hugeDocumentsDetailsItemDto[] = [];
    private gridController = ko.observable<virtualGridController<hugeDocumentsDetailsItemDto>>();
    private columnPreview = new columnPreviewPlugin<hugeDocumentsDetailsItemDto>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        const hugeDocumentsMap = (hint.details() as Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentsDetails).HugeDocuments;
        this.tableItems = _.map(hugeDocumentsMap, x => x);
        
        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<hugeDocumentsDetailsItemDto>(grid, x => x.Id, "Document ID", "30%", {
                    sortable: "string"
                }),
                new textColumn<hugeDocumentsDetailsItemDto>(grid, x => generalUtils.formatBytesToSize(x.Size), "Document size", "20%", {
                    sortable: x => x.Size,
                    defaultSortOrder: "desc"
                }),
                new textColumn<hugeDocumentsDetailsItemDto>(grid, x => generalUtils.formatUtcDateAsLocal(x.Date), "Last access", "50%", {
                    sortable: x => x.Date,
                    defaultSortOrder: "desc"
                }),
            ];
        });

        this.columnPreview.install(".hugeDocumentsDetails", ".js-huge-documents-details-tooltip", 
            (details: hugeDocumentsDetailsItemDto, column: textColumn<hugeDocumentsDetailsItemDto>, e: JQuery.TriggeredEvent, 
             onValue: (context: any, valueToCopy?: string) => void) => {
            const value = column.getCellValue(details);
            if (column.header === "Last access") {
                onValue(moment.utc(details.Date), details.Date);
            } else if (!_.isUndefined(value)) {
                onValue(generalUtils.escapeHtml(value));
            }
        });
    }

    private fetcher(): JQueryPromise<pagedResult<hugeDocumentsDetailsItemDto>> {
        return $.Deferred<pagedResult<hugeDocumentsDetailsItemDto>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "HugeDocuments";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new hugeDocumentsDetails(hint, center));
    }
}

export = hugeDocumentsDetails;
