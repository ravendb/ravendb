/// <reference path="../../../../typings/tsd.d.ts"/>
import app = require("durandal/app");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import columnPreviewPlugin = require("widgets/virtualGrid/columnPreviewPlugin");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import generalUtils = require("common/generalUtils");
import appUrl = require("common/appUrl");
import moment = require("moment");
import indexErrorDetails = require("viewmodels/database/indexes/indexErrorDetails");

type indexErrorInfoModelState = "loading" | "loaded" | "error";

class indexErrorInfoModel {

    dbName: string;
    location: databaseLocationSpecifier;

    gridWasInitialized = false;
    gridId: string;
    
    state = ko.observable<indexErrorInfoModelState>("loading");

    showDetails = ko.observable(false);
    errMsg = ko.observable<string>();
    
    indexErrorsCountDto = ko.observableArray<indexErrorsCount>([]);
    indexErrors = ko.observableArray<IndexErrorPerDocument>([])
    filteredIndexErrors: IndexErrorPerDocument[] = [];
    
    gridController = ko.observable<virtualGridController<IndexErrorPerDocument>>();
    private columnPreview = new columnPreviewPlugin<IndexErrorPerDocument>();

    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;

    totalErrorCount: KnockoutComputed<number>;
    clearErrorsBtnTooltip: KnockoutComputed<string>;
    
    emptyTemplate: KnockoutComputed<string>;
    
    loadingDetailsTask: JQueryDeferred<void>;
    
    constructor(dbName: string, location: databaseLocationSpecifier) {
        this.dbName = dbName;
        this.location = location;
        
        this.initObservables();
    }
    
    initObservables(): void {
        this.totalErrorCount = ko.pureComputed(() => {
            let count = 0;
            const errorsDto = this.indexErrorsCountDto();

            if (errorsDto) {
                for (const error of errorsDto) {
                    count += error.Errors.reduce((count, val) => val.NumberOfErrors + count, 0);
                }
            }

            return count;
        });
        
        this.badgeText = ko.pureComputed(() => {
            if (this.state() === "loading") {
                return "Loading";
            }

            if (this.state() === "error") {
                return "N/A";
            }
            
            return this.totalErrorCount() ? "Errors" : "Ok";
        });
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.state() === "loading") {
                return "state-info";
            }
            
            if (this.state() === "error") {
                return "state-warning";
            }
            return this.totalErrorCount() ? "state-danger" : "state-success";
        });
        
        this.emptyTemplate = ko.pureComputed(() => {
            if (this.state() === "error") {
                return "errored-index-errors-template";
            }
            
            if (this.indexErrors().length > 0 && !this.filteredIndexErrors.length) {
                return "no-matching-index-errors-template";
            }
            
            return "no-index-errors-template";
        })

        this.gridId = `${this.location.nodeTag}-${this.location.shardNumber || "na"}`;
        
        this.clearErrorsBtnTooltip = ko.pureComputed(() => "Click to clear errors from " + generalUtils.formatLocation(this.location));
    }
    
    get gridClass() {
        return `virtual-grid-class-${this.gridId}`;
    }
    
    get tooltipClass() {
        return `js-index-errors-tooltip-${this.gridId}`;
    }
    
    private gridInit() {
        const grid = this.gridController();
        grid.headerVisible(true);
        grid.init(() => this.getIndexErrors(), () =>
            [
                new actionColumn<IndexErrorPerDocument>(grid, (error, index) => this.showErrorDetails(index), "Show", `<i class="icon-preview"></i>`, "72px",
                    {
                        title: () => 'Show indexing error details'
                    }),
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.IndexName, x => appUrl.forEditIndex(x.IndexName, this.dbName), "Index name", "25%", {
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.Document || "n/a", x => x.Document ? appUrl.forEditDoc(x.Document, this.dbName) : null, "Document ID", "20%", {
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new textColumn<IndexErrorPerDocument>(grid, x => generalUtils.formatUtcDateAsLocal(x.Timestamp), "Date", "20%", {
                    sortable: "string"
                }),
                new textColumn<IndexErrorPerDocument>(grid, x => x.Action, "Action", "10%", {
                    sortable: "string"
                }),
                new textColumn<IndexErrorPerDocument>(grid, x => x.Error, "Error", "15%", {
                    sortable: "string"
                })
            ]
        );

        const gridTooltipClass = "." + this.tooltipClass;
        const gridContainerSelector = "." + this.gridClass;
        
        this.columnPreview.install(gridContainerSelector, gridTooltipClass,
            (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQuery.TriggeredEvent,
             onValue: (context: any, valueToCopy?: string) => void) => {
            if (column.header === "Action" || column.header === "Show") {
                // do nothing
            } else if (column.header === "Date") {
                onValue(moment.utc(indexError.Timestamp), indexError.Timestamp);
            } else {
                const value = column.getCellValue(indexError);
                if (value !== undefined) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            }
        });
    }
    
    private getIndexErrors(): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        const deferred = $.Deferred<pagedResult<IndexErrorPerDocument>>();

        this.loadingDetailsTask
            .always(() => {
                deferred.resolve({
                    items: this.filteredIndexErrors,
                    totalResultCount: this.filteredIndexErrors.length
                });
            });
        
        return deferred;
    }
    
    filterAndShow(filter: (item: IndexErrorPerDocument) => boolean) {
        this.filteredIndexErrors = this.indexErrors().filter(filter);

        this.gridController().reset();
    }

    private showErrorDetails(errorIdx: number) {
        const view = new indexErrorDetails(this.filteredIndexErrors, errorIdx);
        app.showBootstrapDialog(view);
    }

    onCountsLoaded(errorsCountDto: indexErrorsCount[]) {
        this.indexErrorsCountDto(errorsCountDto);
        this.state("loaded");
    }

    onCountsLoadError(err: string) {
        this.indexErrors([]);
        this.errMsg(err);
        this.state("error");
    }

    onDetailsLoaded(results: IndexErrorPerDocument[]) {
        this.errMsg("");
        this.indexErrors(results);

        this.loadingDetailsTask.resolve();
    }

    onDetailsLoadError(err: string) {
        this.state("error");
        this.errMsg(err);
        this.filterAndShow(() => false);
        this.loadingDetailsTask.resolve();
    }

    onDetailsLoading() {
        this.loadingDetailsTask = $.Deferred();

        if (!this.gridWasInitialized) {
            this.gridInit();
            this.gridWasInitialized = true;
        } else {
            this.gridController().reset();
        }
    }
}

export = indexErrorInfoModel;
