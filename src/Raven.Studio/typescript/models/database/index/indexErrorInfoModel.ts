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

class indexErrorInfoModel {

    dbName = ko.observable<string>();
    location = ko.observable<databaseLocationSpecifier>();
    
    badgeText: KnockoutComputed<string>;
    badgeClass: KnockoutComputed<string>;
    
    totalErrorCount: KnockoutComputed<number>;
    clearErrorsBtnTooltip: KnockoutComputed<string>;
    
    indexErrorsCountDto = ko.observableArray<indexErrorsCount>();
    filteredIndexErrors = ko.observableArray<IndexErrorPerDocument>();
    
    gridController = ko.observable<virtualGridController<IndexErrorPerDocument>>();
    private columnPreview = new columnPreviewPlugin<IndexErrorPerDocument>();
    
    gridWasInitialized: boolean = false;
    gridId: KnockoutComputed<string>;
    
    showDetails = ko.observable(false);
    errMsg = ko.observable<string>();
    
    constructor(dbName: string, location: databaseLocationSpecifier, errorsCountDto: indexErrorsCount[], errMessage: string = null) {
        this.dbName(dbName);
        this.location(location);
        
        this.indexErrorsCountDto(errorsCountDto);
        this.errMsg(errMessage);
        
        this.initObservables();
    }
    
    initObservables(): void {
        this.totalErrorCount = ko.pureComputed(() => {
            let count = 0;
            const errorsDto = this.indexErrorsCountDto();

            if (errorsDto) {
                for (let i = 0; i < errorsDto.length; i++) {
                    const countToAdd = errorsDto[i].Errors.reduce((count, val) => val.NumberOfErrors + count, 0);
                    count += countToAdd;
                }
            }

            return count;
        });
        
        this.badgeText = ko.pureComputed(() => {
            if (this.errMsg()) {
                return "Not Available";
            }
            return this.totalErrorCount() ? "Errors" : "Ok";
        });
        
        this.badgeClass = ko.pureComputed(() => {
            if (this.errMsg()) {
                return "state-warning";
            }
            return this.totalErrorCount() ? "state-danger" : "state-success";
        });

        this.gridId = ko.pureComputed(() => `${this.location().nodeTag}${this.location().shardNumber || ""}`);
        
        this.clearErrorsBtnTooltip = ko.pureComputed(() => {
            if (this.location().shardNumber !== undefined) {
                return "Click to clear errors from this Shard";
            } else {
                return "Click to clear errors from this Node only";
            }
        });
        
        this.filteredIndexErrors.subscribe(() => {
            if (!this.gridWasInitialized) {
                this.gridInit();
                this.gridWasInitialized = true;
            }

            this.gridController().reset();
        });
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
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.IndexName, x => appUrl.forEditIndex(x.IndexName, this.dbName()), "Index name", "25%", {
                    sortable: "string",
                    customComparator: generalUtils.sortAlphaNumeric
                }),
                new hyperlinkColumn<IndexErrorPerDocument>(grid, x => x.Document, x => appUrl.forEditDoc(x.Document, this.dbName()), "Document Id", "20%", {
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

        const gridTooltipClass = `.js-index-errors-tooltip${this.gridId()}`;
        const gridContainerSelector = `.virtual-grid-class${this.gridId()}`;
        
        this.columnPreview.install(gridContainerSelector, gridTooltipClass,
            (indexError: IndexErrorPerDocument, column: textColumn<IndexErrorPerDocument>, e: JQueryEventObject,
             onValue: (context: any, valueToCopy?: string) => void) => {
            if (column.header === "Action" || column.header === "Show") {
                // do nothing
            } else if (column.header === "Date") {
                onValue(moment.utc(indexError.Timestamp), indexError.Timestamp);
            } else {
                const value = column.getCellValue(indexError);
                if (!_.isUndefined(value)) {
                    onValue(generalUtils.escapeHtml(value), value);
                }
            }
        });
    }
    
    private getIndexErrors(): JQueryPromise<pagedResult<IndexErrorPerDocument>> {
        const deferred = $.Deferred<pagedResult<IndexErrorPerDocument>>();

        return deferred.resolve({
            items: this.filteredIndexErrors(),
            totalResultCount: this.filteredIndexErrors().length
        });
    }

    private showErrorDetails(errorIdx: number) {
        const view = new indexErrorDetails(this.filteredIndexErrors(), errorIdx);
        app.showBootstrapDialog(view);
    }
}

export = indexErrorInfoModel;
