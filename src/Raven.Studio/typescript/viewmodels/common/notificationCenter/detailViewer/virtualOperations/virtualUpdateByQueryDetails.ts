import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import notificationCenter = require("common/notifications/notificationCenter");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import virtualUpdateByQuery = require("common/notifications/models/virtualUpdateByQuery");

class virtualUpdateByQueryDetails extends dialogViewModelBase {

    private virtualNotification: virtualUpdateByQuery;
    private gridController = ko.observable<virtualGridController<queryBasedVirtualBulkOperationItem>>();
    private columnPreview = new columnPreviewPlugin<queryBasedVirtualBulkOperationItem>();
    
    constructor(virtualNotification: virtualUpdateByQuery) {
        super();
        
        this.virtualNotification = virtualNotification;
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            return [
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), "Date", "25%"),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.duration, "Duration (ms)", "15%"),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.items, "Processed documents", "15%"),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.indexOrCollectionUsed, "Collection/Index", "20%"),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.query, "Query", "25%"),
            ];
        });

        this.columnPreview.install(".virtualUpdateByQueryDetails", ".js-virtual-update-by-query-details-tooltip",
            (details: queryBasedVirtualBulkOperationItem,
             column: textColumn<queryBasedVirtualBulkOperationItem>,
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

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<queryBasedVirtualBulkOperationItem>> {
        return $.Deferred<pagedResult<queryBasedVirtualBulkOperationItem>>()
            .resolve({
                items: this.virtualNotification.operations(),
                totalResultCount: this.virtualNotification.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeUpdateByQuery";
    }

    static showDetailsFor(virtualNotification: virtualUpdateByQuery, center: notificationCenter) {
        return app.showBootstrapDialog(new virtualUpdateByQueryDetails(virtualNotification));
    }

}

export = virtualUpdateByQueryDetails;
