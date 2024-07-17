import app = require("durandal/app");

import abstractNotification = require("common/notifications/models/abstractNotification");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import virtualUpdateByQuery = require("common/notifications/models/virtualUpdateByQuery");
import moment = require("moment");

class virtualUpdateByQueryDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/virtualOperations/virtualUpdateByQueryDetails.html");

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

        const dateHeader = "Date";
        const durationHeader = "Duration (ms)";
        
        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.date), dateHeader, "25%", {
                    sortable: x => x.date
                }),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.duration, durationHeader, "15%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.totalItemsProcessed, "Processed documents", "15%", {
                    sortable: "number"
                }),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.indexOrCollectionUsed, "Collection/Index", "20%", {
                    sortable: "string"
                }),
                new textColumn<queryBasedVirtualBulkOperationItem>(grid, x => x.query, "Query", "25%", {
                    sortable: "string"
                }),
            ];
        });

        this.columnPreview.install(".virtualUpdateByQueryDetails", ".js-virtual-update-by-query-details-tooltip",
            (details: queryBasedVirtualBulkOperationItem,
             column: textColumn<queryBasedVirtualBulkOperationItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string | number) => void) => {
                if (!(column instanceof actionColumn)) {
                    if (column.header === dateHeader) {
                        onValue(moment.utc(details.date), details.date);
                    } else if (column.header === durationHeader) {
                        onValue(generalUtils.formatMillis(details.duration), details.duration);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });
    }

    private fetcher(): JQueryPromise<pagedResult<queryBasedVirtualBulkOperationItem>> {
        return $.Deferred<pagedResult<queryBasedVirtualBulkOperationItem>>()
            .resolve({
                items: this.virtualNotification.operations(),
                totalResultCount: this.virtualNotification.operations().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification.type === "CumulativeUpdateByQuery";
    }

    static showDetailsFor(virtualNotification: virtualUpdateByQuery) {
        return app.showBootstrapDialog(new virtualUpdateByQueryDetails(virtualNotification));
    }

}

export = virtualUpdateByQueryDetails;
