/// <reference path="../../../typings/tsd.d.ts"/>

import VirtualRow = require("widgets/virtualGrid/VirtualRow");
import PagedResult = require("widgets/virtualGrid/PagedResult");
import ItemFetch = require("widgets/virtualGrid/ItemFetch");
import Column = require("widgets/virtualGrid/Column");
import TextCellTemplate = require("widgets/virtualGrid/TextCellTemplate");
import CheckedCellTemplate = require("widgets/virtualGrid/CheckedCellTemplate");
import VirtualGridConfig = require("widgets/virtualGrid/VirtualGridConfig");

class VirtualGrid {
    items: Object[] = []; // The items loaded asynchronously.
    totalItemCount: number | null = null;
    virtualRows: VirtualRow[] = []; // These are the fixed number of elements that get displayed on screen. Each virtual row displays an element from .items array. As the user scrolls, rows will be recycled to represent different items.
    fetcher: (skip: number, take: number) => JQueryPromise<PagedResult>; // The function invoked that fetches a chunk of items.
    gridId: string;
    gridElement: JQuery;
    viewportElement: JQuery;
    elementHeight: number;
    virtualHeight = ko.observable(0);
    scrollAnimationFrameHandle = 0;
    isLoading = ko.observable(false);
    queuedFetch: ItemFetch | null = null;
    columns = ko.observableArray<Column>();
    isSelectAllChecked = ko.observable<boolean | null>(false);
    selectedIndices: number[] = [];

    static readonly textCellTemplate = new TextCellTemplate();
    static readonly checkedCellTemplate = new CheckedCellTemplate();
    static readonly minItemFetchCount = 100;

    constructor(params: VirtualGridConfig) {
        this.gridId = "vg-" + (1 + Math.random()).toString().replace(".", "");
        this.fetcher = params.fetcher;
        if (params.columns && params.columns.length > 0) {
            this.columns([this.createCheckedColumn()].concat(params.columns));
        }

        this.isSelectAllChecked.subscribe(allSelected => this.selectAllChanged(allSelected));
        this.fetchItems(0, 100);
        window.requestAnimationFrame(() => this.initializeUIElements());
    }

    private initializeUIElements() {
        this.gridElement = this.findGridElement();
        this.gridElement.on("click", e => this.gridClicked(e));
        this.elementHeight = this.gridElement.height();
        this.virtualRows = this.createVirtualRows();
        this.viewportElement = this.gridElement.find(".viewport");
        this.viewportElement.find(".viewport-scroller").append(this.virtualRows.map(r => r.element[0]));

        this.gridElement.find(".viewport").on("scroll", () => this.gridScrolled());
    }

    private gridScrolled() {
        window.cancelAnimationFrame(this.scrollAnimationFrameHandle);
        this.scrollAnimationFrameHandle = window.requestAnimationFrame(() => this.redraw());
    }

    private redraw() {
        this.layoutVirtualRowPositions();
        this.fillDataIntoRows();
    }

    private findGridElement(): JQuery {
        var element = $(document.querySelector("#" + this.gridId));
        if (element.length === 0) {
            throw new Error("Couldn't find grid element with ID " + this.gridId);
        }

        return element;
    }

    private createVirtualRows(): VirtualRow[] {
        var height = Math.max(100, this.elementHeight);
        var rowsNeededToCoverViewport = Math.ceil(height / VirtualRow.height);
        var desiredRowCount = rowsNeededToCoverViewport * 2;
        var rows: VirtualRow[] = [];
        rows.length = desiredRowCount;
        for (var i = 0; i < desiredRowCount; i++) {
            rows[i] = new VirtualRow();
        }

        return rows;
    }

    private fetchItems(skip: number, take: number) {
        if (this.isLoading()) {
            this.queuedFetch = { skip: skip, take: take };
        } else {
            this.isLoading(true);
            var safeSkip = skip;
            var safeTake = take;
            if (this.totalItemCount != null && skip > this.totalItemCount) {
                skip = this.totalItemCount;
            }
            if (this.totalItemCount != null && (skip + take) > this.totalItemCount) {
                safeTake = this.totalItemCount - skip;
            }

            if (safeTake > 0) {
                this.fetcher(safeSkip, safeTake)
                    .then((results: PagedResult) => this.chunkFetched(results))
                    .fail(error => this.chunkFetchFailed(error, skip, safeTake))
                    .always(() => {
                        // When we're done loading, run the next queued fetch as necessary.
                        this.isLoading(false);
                        this.runQueuedFetch();
                    });
            }
        }
    }

    private runQueuedFetch() {
        if (this.queuedFetch) {
            var { skip, take } = this.queuedFetch;
            this.queuedFetch = null;

            // The previous fetch may have fetched some or all of the items we're about to fetch now.
            // So, before running the queued fetch, modify it to fetch the next chunk of unavailable items.
            var indexOfNextUnavailableChunk = skip;
            for (var i = skip; i < this.items.length; i++) {
                if (!this.items[i]) {
                    indexOfNextUnavailableChunk = i;
                    break;
                }
            }

            this.fetchItems(indexOfNextUnavailableChunk, take);
        }
    }

    private layoutVirtualRowPositions() {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.

        // Determine the view port.
        var scrollTop = this.viewportElement.scrollTop();
        var scrollBottom = scrollTop + this.elementHeight;
        var positionCheck = scrollTop;
        var columns = this.columns();
        var lastPossibleRowY = this.virtualHeight() - VirtualRow.height; // Find out the last possible row in the grid so that we don't place virtual rows beneath this.

        while (positionCheck < scrollBottom && positionCheck <= lastPossibleRowY) {
            var rowAtPosition = this.findRowAtY(positionCheck);
            if (!rowAtPosition) {
                // There's no row at that spot. Find one we can put there.
                rowAtPosition = this.getOffscreenRow(scrollTop, scrollBottom);

                // Populate it with data.
                var rowIndex = Math.floor(positionCheck / VirtualRow.height);
                var isChecked = this.selectedIndices.indexOf(rowIndex) !== -1;
                rowAtPosition.populate(this.items[rowIndex], rowIndex, isChecked, columns);
            }

            var newPositionCheck = rowAtPosition.top + VirtualRow.height;
            if (newPositionCheck <= positionCheck) {
                throw new Error("Virtual grid defect: next position check was smaller or equal to last check, resulting in potentially infinite loop.");
            }

            positionCheck = newPositionCheck;
        }
    }

    fillDataIntoRows() {
        // Find which rows are visible.
        var scrollTop = this.viewportElement.scrollTop();
        var scrollBottom = scrollTop + this.elementHeight;
        var firstVisibleRowIndex: number | null = null;
        var totalVisible = 0;
        var columns = this.columns();
        for (var i = 0; i < this.virtualRows.length; i++) {
            var virtualRow = this.virtualRows[i];
            var isVisible = !virtualRow.isOffscreen(scrollTop, scrollBottom);
            if (isVisible) {
                firstVisibleRowIndex = firstVisibleRowIndex === null ? virtualRow.index : Math.min(virtualRow.index, firstVisibleRowIndex);
                totalVisible++;

                // Fill it with the data we've got loaded. If there's no data, it will display the loading indicator.
                var isRowChecked = this.selectedIndices.indexOf(virtualRow.index) !== -1;
                virtualRow.populate(this.items[virtualRow.index], virtualRow.index, isRowChecked, columns);
            }
        }

        // Of the visible rows, are we missing items for any of them?
        var needsToFetch = false;
        if (firstVisibleRowIndex !== null) {
            for (var i = firstVisibleRowIndex; i < firstVisibleRowIndex + totalVisible; i++) {
                if (!this.items[i]) {
                    needsToFetch = true;
                    break;
                }
            }
        }

        // We're missing items. Fetch them.
        if (needsToFetch) {
            // Take about 50 items before the first visible index (in case we're scrolling up)
            var skip = Math.max(0, firstVisibleRowIndex - 50);
            var take = Math.max(VirtualGrid.minItemFetchCount, totalVisible);
            this.fetchItems(skip, take);
        }
    }

    private findRowAtY(y: number): VirtualRow | null {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.
        for (var i = 0; i < this.virtualRows.length; i++) {
            var vRow = this.virtualRows[i];
            var vRowTop = vRow.top;
            var vRowBottom = vRowTop + VirtualRow.height;
            if (vRowTop <= y && vRowBottom > y) {
                return vRow;
            }
        }

        return null;
    }

    private getOffscreenRow(viewportTop: number, viewportBottom: number): VirtualRow {
        // This is hot path, called multiple times when scrolling.
        // Keep allocations to a minimum.
        for (var i = 0; i < this.virtualRows.length; i++) {
            var row = this.virtualRows[i];
            if (row.isOffscreen(viewportTop, viewportBottom)) {
                return row;
            }
        }

        throw new Error(`Bug: couldn't find an offscreen row to recycle. viewportTop = ${viewportTop}, viewportBottom = ${viewportBottom}, recycle row count = ${this.virtualRows.length}`);
    }

    private chunkFetchFailed(error: any, skip: number, take: number) {
        // Any rows displaying these items, show an error.
        var endIndex = skip + take;
        var failedRows = this.virtualRows
            .filter(r => !r.hasData && r.index >= skip && r.index <= endIndex);
        failedRows.forEach(r => r.dataLoadError(error));
    }

    private chunkFetched(results: PagedResult) {
        if (!this.columns() || this.columns().length === 0) {
            this.assignColumnFromItems(results.items);
        }

        // Add these results to the .items array as necessary.
        this.items.length = results.totalCount;
        this.totalItemCount = results.totalCount;
        this.virtualHeight(results.totalCount * VirtualRow.height);
        var endIndex = results.skip + results.items.length;
        for (var i = 0; i < results.items.length; i++) {
            var rowIndex = i + results.skip;
            this.items[rowIndex] = results.items[i];
        }

        this.redraw();
    }

    private assignColumnFromItems(items: any[]) {
        var propertySet = {};
        items.forEach(i => Object.keys(i).forEach(prop => (<any>propertySet)[prop] = true));

        var columnNames = Object.getOwnPropertyNames(propertySet);
        var viewportWidth = this.viewportElement.prop("clientWidth");
        var columnWidth = Math.floor(viewportWidth / columnNames.length) - CheckedCellTemplate.columnWidth + "px";

        // Put Id and Name columns first.
        var prioritizedColumns = ["Id", "Name"];
        prioritizedColumns
            .reverse()
            .forEach(c => {
                var columnIndex = columnNames.indexOf(c);
                if (columnIndex >= 0) {
                    columnNames.splice(columnIndex, 1);
                    columnNames.unshift(c);
                }
            });

        this.columns([this.createCheckedColumn()].concat(columnNames.map(p => VirtualGrid.propertyNameToColumn(p, columnWidth))));
    }

    /**
     * Creates the system-maintained "checked" column, allowing the user to select rows.
     */
    private createCheckedColumn(): Column {
        return {
            display: `<input class="checked-column-header" type="checkbox" />`,
            dataMemberName: "",
            template: VirtualGrid.checkedCellTemplate,
            width: `${CheckedCellTemplate.columnWidth}px`
        };
    }

    private gridClicked(e: JQueryEventObject) {
        if (e.target) {
            var $target = $(e.target);
            // If we clicked the the checked column header, toggle select all.
            if ($target.hasClass("checked-column-header")) {
                this.isSelectAllChecked(!this.isSelectAllChecked());
            } else if ($target.hasClass("checked-cell-input")) {
                // If we clicked a checked cell, toggle its selected state.
                var rowIndex = this.findRowIndexForCell($target);
                if (rowIndex !== null) {
                    this.toggleRowSelected(rowIndex);
                }
            }
        }
    }

    private findRowIndexForCell(cellElement: JQuery): number | null {
        return this.virtualRows
            .filter(r => r.element.find(cellElement).length > 0)
            .map(r => r.index)[0];
    }

    private toggleRowSelected(rowIndex: number) {
        var selectionIndex = this.selectedIndices.indexOf(rowIndex);
        if (selectionIndex === -1) {
            this.selectedIndices.push(rowIndex);
        } else {
            this.selectedIndices.splice(selectionIndex, 1);
        }

        // If we had all selected or none selected, remove that state.
        if (this.isSelectAllChecked() === true || this.isSelectAllChecked() === false) {
            this.isSelectAllChecked(null);
            $(".checked-column-header").prop("checked", false);
        }

        this.redraw();
    }

    /*
     * Handles when the "select all" observable changes, either by clicking the check column header or via code.
     */
    private selectAllChanged(allSelected: boolean | null) {
        // When selectAll is set to false, remove all selected indices.
        if (allSelected === false) {
            this.selectedIndices.length = 0;
        } else if (allSelected === true) {
            this.selectedIndices.length = this.items.length;
            for (var i = 0; i < this.selectedIndices.length; i++) {
                this.selectedIndices[i] = i;
            }
        }

        // Note: allSelected can be set to null, meaning some items are checked while others are not.

        this.redraw();
    }

    static propertyNameToColumn(propName: string, width: string): Column {
        return {
            display: propName,
            dataMemberName: propName,
            template: VirtualGrid.textCellTemplate,
            width: width
        };
    }
}

// TODO: we may want to move this to an HTML file that's fetched with RequireJS. Knockout components do support this.
ko.components.register("virtual-grid", {
    viewModel: VirtualGrid,
    template: `
<div class="virtual-grid" data-bind="attr: { id: gridId }">
    <!-- Columns -->
    <div class="column-container" data-bind="foreach: columns"><div class="column" data-bind="style: { width: $data.width }"><strong data-bind="html: $data.display"></strong></div></div>    

    <!-- Viewport -->
    <!-- The viewport is the section of the grid showing visible rows -->
    <div class="viewport">

        <!-- The viewport scroller is the very tall scrolling part -->
        <div class="viewport-scroller" data-bind="style: { height: virtualHeight() + 'px' }">

        </div>
    </div>
    
</div>
`
});