import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import generalUtils = require("common/generalUtils");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import moment = require("moment");
import performanceHint = require("common/notifications/models/performanceHint");
import abstractPerformanceHintDetails = require("viewmodels/common/notificationCenter/detailViewer/performanceHint/abstractPerformanceHintDetails");

interface indexReferencesDetailsItem {
    referenceId: string;
    numberOfLoads: number;
}

interface indexingReferencesItem {
    indexName: string;
    lastWarningTime: string;
    details: indexReferencesDetailsItem[];
}

class indexingReferencesDetails extends abstractPerformanceHintDetails {

    view = require("views/common/notificationCenter/detailViewer/performanceHint/indexingReferencesDetails.html");

    currentDetails = ko.observable<indexingReferencesItem>();
    
    tableItems: indexingReferencesItem[] = [];
    private gridController = ko.observable<virtualGridController<indexingReferencesItem>>();
    private columnPreview = new columnPreviewPlugin<indexingReferencesItem>();

    constructor(hint: performanceHint, notificationCenter: notificationCenter) {
        super(hint, notificationCenter);

        const warnings = (this.hint.details() as Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning).Warnings;
        this.tableItems = this.mapDto(warnings);

        // newest first
        this.tableItems.reverse();
    }
    
    private mapDto(details: Record<string, Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning.WarningDetails>): indexingReferencesItem[] {
        return Object.keys(details)
            .map(key => ({
                indexName: key,
                lastWarningTime: details[key].LastWarningTime,
                details: this.mapDetails(details[key].Top10LoadedReferences)
            }));
    }
    
    private mapDetails(details: Record<string, Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning.LoadedReference>): indexReferencesDetailsItem[] {
        return Object.keys(details)
            .map(key => ({
                documentId: key,
                referenceId: details[key].ReferenceId,
                numberOfLoads: details[key].NumberOfLoads
            }));
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            return [
                    new textColumn<indexingReferencesItem>(grid, x => x.indexName, "Index Name", "40%", {
                        sortable: "number"
                    }),
                    new textColumn<indexingReferencesItem>(grid, x => generalUtils.formatUtcDateAsLocal(x.lastWarningTime), "Last Warning Date", "30%", {
                        sortable: x => x.lastWarningTime
                    }),
                    new actionColumn<indexingReferencesItem>(
                        grid, item => this.showDetails(item), "Reference IDs", `Show top 10 loaded references`, "30%",
                        {
                            title: () => 'Show details'
                        })
                ];
        });
        
        this.columnPreview.install(".indexingReferencesDetails", ".js-indexing-references-details-tooltip",
            (details: indexingReferencesItem,
             column: textColumn<indexingReferencesItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                if (!(column instanceof actionColumn)) {
                    if (column.header === "Date") {
                        onValue(moment.utc(details.lastWarningTime), details.lastWarningTime);
                    } else {
                        const value = column.getCellValue(details);
                        if (value) {
                            onValue(generalUtils.escapeHtml(value), value);
                        }
                    }
                }
            });
    }
    
    private showDetails(item: indexingReferencesItem) {
        this.currentDetails(item);
    }
    
    private fetcher(): JQueryPromise<pagedResult<indexingReferencesItem>> {
        return $.Deferred<pagedResult<indexingReferencesItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof performanceHint) && notification.hintType() === "Indexing_References";
    }

    static showDetailsFor(hint: performanceHint, center: notificationCenter) {
        return app.showBootstrapDialog(new indexingReferencesDetails(hint, center));
    }
}

export = indexingReferencesDetails;
