import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualBulkInsert = require("common/notifications/models/virtualBulkInsert");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");

class virtualBulkInsertDetails extends dialogViewModelBase {

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

        grid.init((s, t) => this.fetcher(s, t), () => {
            return [
                new textColumn<virtualBulkOperationItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "40%", {
                    sortable: x => x.date
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.duration, "Duration (ms)", "30%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<virtualBulkOperationItem>(grid, x => x.items, "Inserted documents", "20%", {
                    sortable: "number"
                })
            ];
        });

        this.columnPreview.install(".virtualBulkInsertDetails", ".js-virtual-bulk-insert-details-tooltip",
            (details: virtualBulkOperationItem,
             column: textColumn<virtualBulkOperationItem>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    if (column.header === "Date") {
                        onValue(moment.utc(details.date), details.date);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(value);
                        }
                    }
                }
            });
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<virtualBulkOperationItem>> {
        return $.Deferred<pagedResult<virtualBulkOperationItem>>()
            .resolve({
                items: this.bulkInserts.operations(),
                totalResultCount: this.bulkInserts.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeBulkInsert";
    }

    static showDetailsFor(bulkInserts: virtualBulkInsert, center: notificationCenter) {
        return app.showBootstrapDialog(new virtualBulkInsertDetails(bulkInserts));
    }

}

export = virtualBulkInsertDetails;
