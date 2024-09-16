import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import alert = require("common/notifications/models/alert");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import abstractAlertDetails = require("viewmodels/common/notificationCenter/detailViewer/alerts/abstractAlertDetails");
import ComplexFieldsWarning = Raven.Server.NotificationCenter.Notifications.Details.ComplexFieldsWarning;
import genUtils from "common/generalUtils";

interface WarningItem {
    indexName: string;
    complexFields: string;
}

class complexFieldsAlertDetails extends abstractAlertDetails {
    view = require("views/common/notificationCenter/detailViewer/alerts/complexFieldAlertDetails.html");
    tableItems: WarningItem[] = [];
    private gridController = ko.observable<virtualGridController<WarningItem>>();
    private columnPreview = new columnPreviewPlugin<WarningItem>();

    constructor(alert: alert, notificationCenter: notificationCenter) {
        super(alert, notificationCenter);

        const warning = this.alert.details() as ComplexFieldsWarning;
        
        for (const [index, fields] of Object.entries(warning.Fields ?? [])) {
            this.tableItems.push({indexName: index, complexFields: fields.join(", ")});
        }
    }

    private fetcher(): JQueryPromise<pagedResult<WarningItem>> {
        return $.Deferred<pagedResult<WarningItem>>()
            .resolve({
                items: this.tableItems,
                totalResultCount: this.tableItems.length
            });
    }

    compositionComplete() {
        super.compositionComplete();

        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(() => this.fetcher(), () => {
            const indexNameColumn = new textColumn<WarningItem>(grid, x => x.indexName, "Index Name", "25%", {
                sortable: x => x.indexName
            });
            const indexComplexFields = new textColumn<WarningItem>(grid, x => x.complexFields, "Fields", "25%", {
                sortable: x => x.complexFields
            });
           
            return [indexNameColumn, indexComplexFields];
        });

        this.columnPreview.install(".complexFieldsAlertDetails", ".js-complex-fields-details-tooltip",
            (details: WarningItem,
             column: textColumn<WarningItem>,
             e: JQuery.TriggeredEvent, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(genUtils.escapeHtml(value), value);
                }
            });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof alert) && notification.alertType() === "Indexing_CoraxComplexItem";
    }

    static showDetailsFor(alert: alert, center: notificationCenter) {
        return app.showBootstrapDialog(new complexFieldsAlertDetails(alert, center));
    }
}

export = complexFieldsAlertDetails;
