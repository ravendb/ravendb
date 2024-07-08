import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import virtualBulkInsert = require("common/notifications/models/virtualBulkInsert");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import moment = require("moment");

class virtualBulkInsertDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/virtualOperations/virtualBulkInsertDetails.html");

    private bulkInserts: virtualBulkInsert;
    private gridController = ko.observable<virtualGridController<virtualBulkOperationItem>>();
    private columnPreview = new columnPreviewPlugin<virtualBulkOperationItem>();
    
    constructor(bulkInserts: virtualBulkInsert) {
        super();
        
        this.bulkInserts = bulkInserts;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<virtualBulkOperationItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "25%", {
                    sortable: x => x.date
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.duration, "Duration (ms)", "15%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.documentsProcessed, "Documents", "15%", {
                    sortable: "number"
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.attachmentsProcessed, "Attachments", "15%", {
                    sortable: "number"
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.countersProcessed, "Counters", "15%", {
                    sortable: "number"
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.timeSeriesProcessed, "Time Series", "15%", {
                    sortable: "number"
                })
            ];
        });

        this.columnPreview.install(".virtualBulkInsertDetails", ".js-virtual-bulk-insert-details-tooltip",
            (details: virtualBulkOperationItem,
             column: textColumn<virtualBulkOperationItem>,
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

    private fetcher(): JQueryPromise<pagedResult<virtualBulkOperationItem>> {
        return $.Deferred<pagedResult<virtualBulkOperationItem>>()
            .resolve({
                items: this.bulkInserts.operations(),
                totalResultCount: this.bulkInserts.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeBulkInsert";
    }

    static showDetailsFor(bulkInserts: virtualBulkInsert) {
        return app.showBootstrapDialog(new virtualBulkInsertDetails(bulkInserts));
    }

}

export = virtualBulkInsertDetails;
