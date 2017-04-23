import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");

type pagingDetailsItemDto = {
    Action: string;
    NumberOfResults: number;
    PageSize: number;
    Occurence: string;
}

class pagingDetails extends abstractPerformanceHintDetails {

    tableItems = [] as pagingDetailsItemDto[];
    private gridController = ko.observable<virtualGridController<pagingDetailsItemDto>>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        this.tableItems = this.mapItems(hint.details() as Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails);
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            return [
                new textColumn<pagingDetailsItemDto>(x => x.Action, "Action", "30%"),
                new textColumn<pagingDetailsItemDto>(x => x.NumberOfResults ? x.NumberOfResults.toLocaleString() : 'n/a', "# of results", "20%"),
                new textColumn<pagingDetailsItemDto>(x => x.PageSize ? x.PageSize.toLocaleString() : 'n/a', "Page size", "20%"),
                new textColumn<pagingDetailsItemDto>(x => x.Occurence, "Date", "30%")
            ];
        });
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<pagingDetailsItemDto>> {
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
                    Occurence: item.Occurence,
                    PageSize: item.PageSize
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
