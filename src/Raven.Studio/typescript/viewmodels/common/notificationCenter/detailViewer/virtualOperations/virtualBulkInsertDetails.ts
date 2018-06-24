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
    private gridController = ko.observable<virtualGridController<virtualBulkInsertItem>>();
    private columnPreview = new columnPreviewPlugin<virtualBulkInsertItem>();
    
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
                new textColumn<virtualBulkInsertItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "40%"),
                new textColumn<virtualBulkInsertItem>(grid, x => x.duration, "Duration (ms)", "30%"),
                new textColumn<virtualBulkInsertItem>(grid, x => x.items, "Inserted documents", "20%")
            ];
        });

        this.columnPreview.install(".virtualBulkInsertDetails", ".js-virtual-bulk-insert-details-tooltip",
            (details: virtualBulkInsertItem,
             column: textColumn<virtualBulkInsertItem>,
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

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<virtualBulkInsertItem>> {
        return $.Deferred<pagedResult<virtualBulkInsertItem>>()
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
