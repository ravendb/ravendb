import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");

type gridItem = {
    Id: string;
    Description: string;
}

class enforceRevisionsConfigurationDetail extends abstractOperationDetails {
    progress: KnockoutObservable<Raven.Client.Documents.Operations.Revisions.EnforceConfigurationResult>;

    private gridController = ko.observable<virtualGridController<gridItem>>();
    private columnPreview = new columnPreviewPlugin<gridItem>();
    private allWarnings = ko.observableArray<gridItem>([]);
    
    private gridInitialized = false;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        
        this.initObservables();
    }

    initObservables() {
        super.initObservables();
        
        this.progress = ko.pureComputed(() => {
            const progressResults = this.op.status() === "Completed" ? this.op.result() : this.op.progress();
            return progressResults as Raven.Client.Documents.Operations.Revisions.EnforceConfigurationResult;
        });
    }

    compositionComplete() {
        super.compositionComplete();

        this.progress.subscribe(result => {
            if (result) {
                this.allWarnings(_.map(result.Warnings, (value, key) => {
                    return {
                        Id: key,
                        Description: value
                    } as gridItem
                }));

                if (this.allWarnings().length) {
                    if (!this.gridInitialized) {
                        this.initGrid();
                    }
                    
                    this.gridController().reset(false);    
                }
            }
        });
    }
    
    private initGrid() {
        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init((s, t) => this.fetcher(s, t), () => {
            return [
                new textColumn<gridItem>(grid, x => x.Id, "Document ID", "15%", {
                    sortable: "string"
                }),
                new textColumn<gridItem>(grid, x => x.Description, "Warning", "85%", {
                    sortable: "string"
                })
            ];
        });

        this.columnPreview.install(".revisionsDetails", ".js-revisions-details-tooltip",
            (details: gridItem,
             column: textColumn<gridItem>,
             e: JQueryEventObject, onValue: (context: any, valueToCopy?: string) => void) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(generalUtils.escapeHtml(value));
                }
            });
        
        this.gridInitialized = true;
    }

    private fetcher(skip: number, take: number): JQueryPromise<pagedResult<gridItem>> {
        return $.Deferred<pagedResult<gridItem>>()
            .resolve({
                items: this.allWarnings(),
                totalResultCount: this.allWarnings().length
            });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "EnforceRevisionConfiguration";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new enforceRevisionsConfigurationDetail(op, center));
    }
}

export = enforceRevisionsConfigurationDetail;
