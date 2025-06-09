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

type pagingDetailsItemDto = {
    Action: string;
    NumberOfResults: number;
    PageSize: number;
    Occurrence: string;
    Duration: number;
    Details: string;
    TotalDocumentsSizeInBytes: number;
}

class pagingDetails extends abstractPerformanceHintDetails {

    view = require("views/common/notificationCenter/detailViewer/performanceHint/pagingDetails.html");

    tableItems: pagingDetailsItemDto[] = [];
    private gridController = ko.observable<virtualGridController<pagingDetailsItemDto>>();
    private columnPreview = new columnPreviewPlugin<pagingDetailsItemDto>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        this.tableItems = this.mapItems(hint.details() as Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails);
        
        // newest first
        this.tableItems.reverse();
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                new textColumn<pagingDetailsItemDto>(grid, x => x.Action, "Action", "13%", {
                    sortable: "string"
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => x.NumberOfResults ? x.NumberOfResults.toLocaleString() : 'n/a', "# of results", "10%", {
                    sortable: x => x.NumberOfResults,
                    defaultSortOrder: "desc"
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => x.PageSize ? x.PageSize.toLocaleString() : 'n/a', "Page size", "12%", {
                    sortable: x => x.PageSize,
                    defaultSortOrder: "desc"
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => x.TotalDocumentsSizeInBytes ? generalUtils.formatBytesToSize(x.TotalDocumentsSizeInBytes) : 'n/a', "Docs size", "12%", {
                    sortable: x => x.TotalDocumentsSizeInBytes,
                    defaultSortOrder: "desc"
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => generalUtils.formatUtcDateAsLocal(x.Occurrence), "Date", "13%", {
                    sortable: x => x.Occurrence
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => x.Duration, "Duration (ms)", "10%", {
                    sortable: "number",
                    defaultSortOrder: "desc"
                }),
                new textColumn<pagingDetailsItemDto>(grid, x => x.Details, "Details", "30%")
            ];
        });

        this.columnPreview.install(".pagingDetails", ".js-paging-details-tooltip", 
            (details: pagingDetailsItemDto, column: textColumn<pagingDetailsItemDto>, e: JQuery.TriggeredEvent, 
             onValue: (context: any, valueToCopy?: string) => void) => {
            const value = column.getCellValue(details);
            if (column.header === "Date") {
                onValue(moment.utc(details.Occurrence), details.Occurrence);
            } else if (!_.isUndefined(value)) {
                onValue(generalUtils.escapeHtml(value), value);
            }
        });
    }

    private fetcher(): JQueryPromise<pagedResult<pagingDetailsItemDto>> {
        return $.Deferred<pagedResult<pagingDetailsItemDto>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    private mapItems(details: Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails): pagingDetailsItemDto[] {
        return _.flatMap(details.Actions, (value, key) => {
            return value.map(item => 
                ({
                    Action: key,
                    NumberOfResults: item.NumberOfResults,
                    Occurrence: item.Occurrence,
                    PageSize: item.PageSize,
                    Duration: item.Duration,
                    Details: item.Details,
                    TotalDocumentsSizeInBytes: item.TotalDocumentsSizeInBytes
                } as pagingDetailsItemDto));
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "Paging";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new pagingDetails(hint, center));
    }
}

export = pagingDetails;
