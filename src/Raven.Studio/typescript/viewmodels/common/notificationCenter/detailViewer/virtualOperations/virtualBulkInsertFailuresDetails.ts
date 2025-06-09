import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import virtualBulkInsertFailures = require("common/notifications/models/virtualBulkInsertFailures");
import moment = require("moment");

class virtualBulkInsertFailuresDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/virtualOperations/virtualBulkInsertFailuresDetails.html");

    private bulkInserts: virtualBulkInsertFailures;
    private gridController = ko.observable<virtualGridController<virtualBulkOperationFailureItem>>();
    private columnPreview = new columnPreviewPlugin<virtualBulkOperationFailureItem>();
    
    constructor(bulkInserts: virtualBulkInsertFailures) {
        super();
        
        this.bulkInserts = bulkInserts;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<virtualBulkOperationFailureItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "25%", {
                    sortable: x => x.date
                }),
                new textColumn<virtualBulkOperationFailureItem>(grid, x => x.duration, "Duration (ms)", "15%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<virtualBulkOperationFailureItem>(grid, x => x.errorMsg, "Error message", "20%", {
                    sortable: "string"
                }),
                new textColumn<virtualBulkOperationFailureItem>(grid, x => x.error, "Error", "40%", {
                    sortable: "string"
                })
            ];
        });

        this.columnPreview.install(".virtualBulkInsertFailuresDetails", ".js-virtual-bulk-insert-failures-details-tooltip",
            (details: virtualBulkOperationFailureItem,
             column: textColumn<virtualBulkOperationFailureItem>,
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

    private fetcher(): JQueryPromise<pagedResult<virtualBulkOperationFailureItem>> {
        return $.Deferred<pagedResult<virtualBulkOperationFailureItem>>()
            .resolve({
                items: this.bulkInserts.operations(),
                totalResultCount: this.bulkInserts.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeBulkInsertFailures";
    }

    static showDetailsFor(bulkInserts: virtualBulkInsertFailures) {
        return app.showBootstrapDialog(new virtualBulkInsertFailuresDetails(bulkInserts));
    }

}

export = virtualBulkInsertFailuresDetails;
