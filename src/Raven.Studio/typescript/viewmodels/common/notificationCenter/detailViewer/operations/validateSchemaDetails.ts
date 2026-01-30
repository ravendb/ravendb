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
    Message: string;
};

class validateSchemaDetails extends abstractOperationDetails {
    view = require("views/common/notificationCenter/detailViewer/operations/validateSchemaDetails.html");

    progress: KnockoutObservable<ValidateSchemaResult>;

    private gridController = ko.observable<virtualGridController<gridItem>>();
    private columnPreview = new columnPreviewPlugin<gridItem>();
    private allErrors = ko.observableArray<gridItem>([]);

    private gridInitialized = false;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.initObservables();
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return notification instanceof operation && notification.taskType() === "ValidateSchema";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new validateSchemaDetails(op, center));
    }

    compositionComplete() {
        super.compositionComplete();

        const initialResult = this.progress();
        if (initialResult) {
            this.updateErrors(initialResult);
            this.ensureGridInitialized();
        }

        this.progress.subscribe((result) => {
            if (result) {
                this.updateErrors(result);
                this.ensureGridInitialized();
                this.refreshGrid();
            }
        });
    }

    protected initObservables() {
        super.initObservables();

        this.progress = ko.pureComputed(() => {
            const progressResults = this.op.status() === "Completed" ? this.op.result() : this.op.progress();

            if (this.op.status() === "Completed") {
                this.allErrors(
                    Object.entries((this.op.result() as ValidateSchemaResult).Errors ?? {}).map(
                        ([key, value]): gridItem => ({
                            Id: key,
                            Message: value,
                        })
                    )
                );
            }

            return progressResults as ValidateSchemaResult;
        });
    }

    private ensureGridInitialized() {
        if (!this.gridInitialized) {
            this.initGrid();
        }
    }

    private refreshGrid() {
        const grid = this.gridController();
        if (grid) {
            grid.reset(true);
        }
    }

    private updateErrors(result: ValidateSchemaResult) {
        this.allErrors(
            Object.entries(result.Errors ?? {}).map(([key, value]): gridItem => ({
                Id: key,
                Message: value,
            }))
        );
    }

    private initGrid() {
        const grid = this.gridController();
        grid.headerVisible(true);

        grid.init(
            () => this.fetcher(),
            () => {
                return [
                    new textColumn<gridItem>(grid, (x) => x.Id, "Document ID", "30%", {
                        sortable: "string",
                    }),
                    new textColumn<gridItem>(grid, (x) => x.Message, "Error", "70%", {
                        sortable: "string",
                    }),
                ];
            }
        );

        this.columnPreview.install(
            ".validateSchemaDetails",
            ".js-validate-schema-tooltip",
            (
                details: gridItem,
                column: textColumn<gridItem>,
                e: JQuery.TriggeredEvent,
                onValue: (context: any, valueToCopy?: string) => void
            ) => {
                const value = column.getCellValue(details);
                if (value) {
                    onValue(generalUtils.escapeHtml(value));
                }
            }
        );

        this.gridInitialized = true;
    }

    private fetcher(): JQueryPromise<pagedResult<gridItem>> {
        return $.Deferred<pagedResult<gridItem>>().resolve({
            items: this.allErrors(),
            totalResultCount: this.allErrors().length,
        });
    }
}

export = validateSchemaDetails;
