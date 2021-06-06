import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");

interface indexingDetailsItemDto extends Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument.WarningDetails {
    IndexName: string;
}

class indexingDetails extends abstractPerformanceHintDetails {

    tableItems = [] as indexingDetailsItemDto[];
    suggestions = ko.observableArray<string>([]);
    
    private gridController = ko.observable<virtualGridController<indexingDetailsItemDto>>();
    private columnPreview = new columnPreviewPlugin<indexingDetailsItemDto>();
    
    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        this.tableItems = this.mapItems(hint.details() as Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument);

        // newest first
        this.tableItems.reverse();
        
        const suggestionsSet = new Set<string>();
        this.tableItems.map(x => suggestionsSet.add(x.Suggestion));
        this.suggestions(Array.from(suggestionsSet));
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            return [
                new textColumn<indexingDetailsItemDto>(grid, x => x.IndexName, "Index Name", "20%", {
                    sortable: "string"
                }),
                new textColumn<indexingDetailsItemDto>(grid, x => x.NumberOfExceedingDocuments ? x.NumberOfExceedingDocuments.toLocaleString() : 'n/a', "# of Exceeding Documents", "20%", {
                    sortable: x => x.NumberOfExceedingDocuments,
                    defaultSortOrder: "desc"
                }),
                new textColumn<indexingDetailsItemDto>(grid, x => x.SampleDocumentId ? x.SampleDocumentId : 'n/a', "Sample Document ID", "20%", {
                    sortable: "string"
                }),
                new textColumn<indexingDetailsItemDto>(grid, x => x.MaxNumberOutputsPerDocument ? x.MaxNumberOutputsPerDocument.toLocaleString() : 'n/a', "# of Outputs per Document", "20%", {
                    sortable: x => x.MaxNumberOutputsPerDocument,
                    defaultSortOrder: "desc"
                }),
                new textColumn<indexingDetailsItemDto>(grid, x => generalUtils.formatUtcDateAsLocal(x.LastWarningTime), "Date", "20%", {
                    sortable: x => x.LastWarningTime
                })
            ];
        });

        this.columnPreview.install(".indexingDetails", ".js-indexing-details-tooltip",
            (details: indexingDetailsItemDto, column: textColumn<indexingDetailsItemDto>, e: JQueryEventObject,
             onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (column.header === "Date") {
                    onValue(moment.utc(details.LastWarningTime), details.LastWarningTime);
                } else if (!_.isUndefined(value)) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            });
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<indexingDetailsItemDto>> {
        return $.Deferred<pagedResult<indexingDetailsItemDto>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    private mapItems(details: Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument): indexingDetailsItemDto[] {
        return _.flatMap(details.Warnings, (value, key) => {
            return value.map(item => ({ IndexName: key, ...item }));
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "Indexing";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new indexingDetails(hint, center));
    }
}

export = indexingDetails;
