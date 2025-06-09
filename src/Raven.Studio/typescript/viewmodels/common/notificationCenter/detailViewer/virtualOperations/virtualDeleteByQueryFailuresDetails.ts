import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import virtualDeleteByQueryFailures = require("common/notifications/models/virtualDeleteByQueryFailures");
import moment = require("moment");

class virtualDeleteByQueryFailuresDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/virtualOperations/virtualDeleteByQueryFailuresDetails.html");

    private virtualNotification: virtualDeleteByQueryFailures;
    private gridController = ko.observable<virtualGridController<queryBasedVirtualBulkOperationFailureItem>>();
    private columnPreview = new columnPreviewPlugin<queryBasedVirtualBulkOperationFailureItem>();

    constructor(virtualNotification: virtualDeleteByQueryFailures) {
        super();

        this.virtualNotification = virtualNotification;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<queryBasedVirtualBulkOperationFailureItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "25%", {
                    sortable: x => x.date
                }),
                new textColumn<queryBasedVirtualBulkOperationFailureItem>(grid, x => x.duration, "Duration (ms)", "15%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<queryBasedVirtualBulkOperationFailureItem>(grid, x => x.query, "Query", "25%", {
                    sortable: "string"
                }),
                new textColumn<queryBasedVirtualBulkOperationFailureItem>(grid, x => x.errorMsg, "Error message", "15%", {
                    sortable: "string"
                }),
                new textColumn<queryBasedVirtualBulkOperationFailureItem>(grid, x => x.error, "Error", "20%", {
                    sortable: "string"
                })
            ];
        });

        this.columnPreview.install(".virtualDeleteByQueryFailuresDetails", ".js-virtual-delete-by-query-failures-details-tooltip",
            (details: queryBasedVirtualBulkOperationFailureItem,
             column: textColumn<queryBasedVirtualBulkOperationFailureItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    if (column.header === "Date") {
                        onValue(moment.utc(details.date), details.date);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });
    }

    private fetcher(): JQueryPromise<pagedResult<queryBasedVirtualBulkOperationFailureItem>> {
        return $.Deferred<pagedResult<queryBasedVirtualBulkOperationFailureItem>>()
            .resolve({
                items: this.virtualNotification.operations(),
                totalResultCount: this.virtualNotification.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeDeleteByQueryFailures";
    }

    static showDetailsFor(virtualNotification: virtualDeleteByQueryFailures) {
        return app.showBootstrapDialog(new virtualDeleteByQueryFailuresDetails(virtualNotification));
    }

}

export = virtualDeleteByQueryFailuresDetails;
