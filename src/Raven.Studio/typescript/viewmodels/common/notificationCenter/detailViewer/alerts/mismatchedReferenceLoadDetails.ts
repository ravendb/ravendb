import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import MismatchedReferencesLoadWarning = Raven.Server.NotificationCenter.Notifications.Details.MismatchedReferencesLoadWarning;
import genUtils from "common/generalUtils";

interface WarningItem {
    actualCollection: string;
    mismatchedCollections: string[];
    referenceId: string;
    sourceId: string;
}

class mismatchedReferenceLoadDetails extends abstractAlertDetails {
    
    view = require("views/common/notificationCenter/detailViewer/alerts/mismatchedReferenceLoadDetails.html");

    tableItems: WarningItem[] = [];
    private gridController = ko.observable<virtualGridController<WarningItem>>();
    private columnPreview = new columnPreviewPlugin<WarningItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const warning = this.alert.details() as MismatchedReferencesLoadWarning;
        
        Object.values(warning.Warnings).forEach(perSourceList => {
            this.tableItems.push(...perSourceList.map(x => ({
                actualCollection: x.ActualCollection,
                mismatchedCollections: x.MismatchedCollections,
                referenceId: x.ReferenceId,
                sourceId: x.SourceId
            })))
        });
    }
    
    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const sourceIdColumn = new textColumn<WarningItem>(grid, x => x.sourceId, "Source ID", "25%", {
                sortable: x => x.sourceId
            });
            const referenceId = new textColumn<WarningItem>(grid, x => x.referenceId, "Reference ID", "25%", {
                sortable: x => x.referenceId
            });
            const actualCollectionColumn = new textColumn<WarningItem>(grid, x => x.actualCollection, "Actual Collection", "25%", {
                sortable: x => x.actualCollection
            });
            const mismatchedCollectionColumn = new textColumn<WarningItem>(grid, x => x.mismatchedCollections.join(", "), "Mismatched collections", "25%");

            return [sourceIdColumn, referenceId, actualCollectionColumn, mismatchedCollectionColumn];
        });
        
        this.columnPreview.install(".mismatchedReferenceLoadDetails", ".js-mismatched-reference-load-tooltip",
            (details: WarningItem,
             column: textColumn<WarningItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    private fetcher(): JQueryPromise<pagedResult<WarningItem>> {
        return $.Deferred<pagedResult<WarningItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "MismatchedReferenceLoad";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new mismatchedReferenceLoadDetails(alert, center));
    }
}

export = mismatchedReferenceLoadDetails;
