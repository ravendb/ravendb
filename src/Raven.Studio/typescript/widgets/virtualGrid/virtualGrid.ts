/// <reference path="../../../typings/tsd.d.ts"/>

import virtualRow = require("widgets/virtualGrid/virtualRow");
import itemFetch = require("widgets/virtualGrid/itemFetch");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import sortableVirtualColumn = require("widgets/virtualGrid/columns/sortableVirtualColumn");
import virtualGridConfig = require("widgets/virtualGrid/virtualGridConfig");
import actionColumn = require("widgets/virtualGrid/columns/actionColumn");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualGridUtils = require("widgets/virtualGrid/virtualGridUtils");
import virtualGridSelection = require("widgets/virtualGrid/virtualGridSelection");
import shiftSelectionPreview = require("widgets/virtualGrid/shiftSelectionPreview");

class virtualGrid<T> {

    private items = new Map<number, T>(); // The items loaded asynchronously.
    private totalItemCount: number | null = null;
    private virtualRows: virtualRow[] = []; // These are the fixed number of elements that get displayed on screen. Each virtual row displays an element from .items array. As the user scrolls, rows will be recycled to represent different items.
    private gridId: string;
    private $gridElement: JQuery;
    private $viewportElement: JQuery;
    private $columnContainer: JQuery;
    private gridElementHeight: number;
    private virtualHeight = ko.observable(0);
    private virtualWidth = ko.observable<number>();
    private scrollAnimationFrameHandle = 0;
    private isLoading = ko.observable(false);
    private emptyResult = ko.observable(false);
    private emptyTemplate: string = null;
    private queuedFetch: itemFetch | null = null;
    private columns = ko.observableArray<virtualColumn>();
    private isGridVisible = false;
    private selectionDiff: number[] = [];
    private inIncludeSelectionMode: boolean = true;
    
    private sortByColumn = ko.observable<sortableVirtualColumn>();
    private sortMode = ko.observable<sortMode>("asc");
    
    private defaultSortByColumn = ko.observable<number>(-1);
    private defaultSortMode = ko.observable<sortMode>("asc");

    private dirtyResults = ko.observable<boolean>(false);
    private previousResultsEtag = ko.observable<string>();

    private selection = ko.observable<virtualGridSelection<T>>();
    private shiftSelection: shiftSelectionPreview;

    private settings = new virtualGridConfig();
    private controller: virtualGridController<T>;
    private previousScroll: [number, number] = [0, 0];
    private previousWindowSize: [number, number] = [0, 0];
    private condensed = false;
    private rowHeight: number;

    private static readonly minItemFetchCount = 100;
    private static readonly viewportSelector = ".viewport";
    private static readonly columnContainerSelector = ".column-container";
    private static readonly viewportScrollerSelector = ".viewport-scroller";
    private static readonly minColumnWidth = 20;

    constructor(params: { controller: KnockoutObservable<virtualGridController<T>>, emptyTemplate: string , condensed: boolean}) {
        this.gridId = _.uniqueId("vg_");

        this.refreshSelection();

        this.initController();

        if (params.controller) {
            params.controller(this.controller);
        }

        if (params.emptyTemplate) {
            this.emptyTemplate = params.emptyTemplate;
        }
        
        if (params.condensed) {
            this.condensed = true;
            this.rowHeight = 24;
        } else {
            this.rowHeight = 36;
        }
    }

    private initController() {
        this.controller = {
            findRowForCell: cell => this.findRowForCell(cell),
            headerVisible: v => this.settings.showHeader(v),
            init: (fetcher, columnsProvider) => this.init(fetcher, columnsProvider),
            reset: (hard: boolean = true, retainSort: boolean = true) => this.resetItems(hard, retainSort),
            selection: this.selection,
            findItem: (predicate) => this.findItem(predicate),
            getSelectedItems: () => this.getSelectedItems(),
            setSelectedItems: (selection: Array<T>) => this.setSelectedItems(selection),
            dirtyResults: this.dirtyResults,
            resultEtag: () => this.previousResultsEtag(),
            scrollDown: () => this.scrollDown(),
            setDefaultSortBy: (columnIndex, mode) => this.setDefaultSortBy(columnIndex, mode)
        }
    }

    private init(fetcher: (skip: number, take: number) => JQueryPromise<pagedResult<T>>, columnsProvider: (containerWidth:number, results: pagedResult<T>) => virtualColumn[]) {
        this.settings.fetcher = fetcher;
        this.settings.columnsProvider = columnsProvider;

        this.fetchItems(0, 100);
    }

    dispose() {
        this.shiftSelection.dispose();
    }

    // Called by Knockout once the grid has been rendered.
    afterRender() {
        this.initializeUIElements();
    }

    private initializeUIElements() {
        this.$gridElement = this.findGridElement();
        this.gridElementHeight = this.$gridElement.height();
        this.$gridElement.on("click", e => this.gridClicked(e));
        this.$viewportElement = this.$gridElement.find(virtualGrid.viewportSelector);
        this.$columnContainer = this.$gridElement.find(virtualGrid.columnContainerSelector);
        this.initializeVirtualRows();
        this.$viewportElement.on("scroll", () => this.gridScrolled());

        this.$gridElement.on("mousedown.columnResize", ".column", e => this.handleResize(e));
        this.$gridElement.on("mousedown.columnResize", ".cell", e => this.handleResize(e));
        
        this.$gridElement.on("click.sort", ".column.sortable", e => this.handleSort(e));

        this.shiftSelection = new shiftSelectionPreview(this.gridId, () => this.virtualRows, (s, e) => this.checkIfAllRecordsInRangeAreLoaded(s, e));
        this.shiftSelection.init();
    }

    private checkIfAllRecordsInRangeAreLoaded(start: number, end: number): boolean {
        if (start > end) {
            throw new Error("invalid range");
        }
        const items = this.items;
        for (let i = start; i < end; i++) {
            if (!items.has(i))
                return false;
        }
        return true;
    }

    private setDefaultSortBy(columnIndex: number, mode: sortMode = null) {
        this.defaultSortByColumn(columnIndex);
        this.defaultSortMode(mode || "asc");
    }
    
    private handleSort(e: JQueryEventObject) {
        const columnIndex = $(e.currentTarget).index();
        if (columnIndex < 0) {
            return;
        }
        const columnToUse = this.columns()[columnIndex] as sortableVirtualColumn;
        if (!columnToUse.sortProvider) {
            return;
        }
        
        if (this.sortByColumn() !== columnToUse) {
            this.sortByColumn(columnToUse);
            this.sortMode(columnToUse.defaultSortOrder);
        } else {
            this.sortMode(this.sortMode() === "asc" ? "desc" : "asc");
        }
        
        this.sortItems();
        this.render();
    }
    
    private sortItems() {
        if (!this.sortByColumn()) {
            // try to use default
            const defaultColumnIndex = this.defaultSortByColumn();
            if (defaultColumnIndex > -1) {
                const columnToUse = this.columns()[defaultColumnIndex];
                this.sortByColumn(columnToUse);
                this.sortMode(this.defaultSortMode());
            }
        }
        
        let columnToUse = this.sortByColumn();
        
        if (columnToUse) {
            let itemsToSort = Array.from(this.items.values());
            const sortProvider = columnToUse.sortProvider(this.sortMode());
            if (sortProvider) {
                itemsToSort = sortProvider(itemsToSort);

                this.items.clear();
                itemsToSort.forEach((v, idx) => {
                    this.items.set(idx, v);
                });
            }
        }
    }

    private handleResize(e: JQueryEventObject) {
        // since resize handles are pseudo html elements, we get invalid target
        // check click location to distinguish between handle and title click
        if (e.offsetX > 8) {
            return;
        }

        // Stop propagation of the event so the text selection doesn't fire up
        if (e.stopPropagation) e.stopPropagation();
        if (e.preventDefault) e.preventDefault();
        e.cancelBubble = true;
        e.returnValue = false;

        const $document = $(document);
        const targetColumnIdx = $(e.target).index();
        const columnIndex = targetColumnIdx - 1;
        if (columnIndex < 0) {
            return;
        }
        const columnToResize = this.columns()[columnIndex];
        const startX = e.pageX;
        const columnWidthInPixels = virtualGridUtils.widthToPixels(columnToResize);

        $document.on("mousemove.columnResize", e => {
            const dx = e.pageX - startX;
            const requestedWidth = columnWidthInPixels + dx;
            const currentWidth = Math.max(requestedWidth, virtualGrid.minColumnWidth) + "px";
            $(`.column-container .column:eq(${columnIndex})`, this.$gridElement).innerWidth(currentWidth);
            $(`.viewport .virtual-row .cell:nth-child(${columnIndex + 1})`, this.$gridElement).innerWidth(currentWidth);
        });

        $document.on("mouseup.columnResize", e => {
            const dx = e.pageX - startX;
            const requestedWidth = columnWidthInPixels + dx;
            // write back new width as css value
            columnToResize.width = Math.max(requestedWidth, virtualGrid.minColumnWidth) + "px";
            this.syncVirtualWidth();

            $document.off("mousemove.columnResize");
            $document.off("mouseup.columnResize");
        });
    }

    private initializeVirtualRows() {
        this.virtualRows = this.createVirtualRows();
        this.$viewportElement
            .find(virtualGrid.viewportScrollerSelector)
            .empty()
            .append(this.virtualRows.map(r => r.element[0]));
    }

    private createVirtualRows(): virtualRow[] {
        const height = Math.max(100, this.gridElementHeight);
        const rowsNeededToCoverViewport = Math.ceil(height / this.rowHeight);
        const desiredRowCount = rowsNeededToCoverViewport * 2;
        const rows: virtualRow[] = [];
        rows.length = desiredRowCount;
        for (let i = 0; i < desiredRowCount; i++) {
            rows[i] = new virtualRow(this.rowHeight);
        }

        return rows;
    }

    private gridScrolled() {
        if (this.totalItemCount != null) {
            const currentScroll = [this.$viewportElement.scrollTop(), this.$viewportElement.scrollLeft()] as [number, number];

            // horizontal scroll
            if (currentScroll[1] !== this.previousScroll[1]) {
                this.syncHeaderShift();
            }

            // vertical scroll
            if (currentScroll[0] !== this.previousScroll[0]) {
                window.cancelAnimationFrame(this.scrollAnimationFrameHandle);
                this.scrollAnimationFrameHandle = window.requestAnimationFrame(() => this.render());
            }

            this.previousScroll = currentScroll;
        }
    }

    private render() {
        // The grid may not be visible if the results returned quickly and we haven't finished initializing the UI.
        // In such a case, we queue up a render to occur later.
        if (this.checkGridVisibility()) {
            this.checkForUpdatedGridHeight();
            this.layoutVirtualRowPositions();
            this.fillDataIntoRows();
        } else {
            throw new Error("Grid is not visible!");
        }
    }

    private findGridElement(): JQuery {
        const element = $(document.querySelector("#" + this.gridId));
        if (element.length === 0) {
            throw new Error("Couldn't find grid element with ID " + this.gridId);
        }

        return element;
    }

    private fetchItems(skip: number, take: number): void {
        if (this.isLoading()) {
            this.queuedFetch = { skip: skip, take: take };
        } else {
            const [safeSkip, safeTake] = this.makeSkipAndTakeSafe(skip, take);

            if (safeTake > 0) {
                this.isLoading(true);

                const fetcherTask = this.settings.fetcher(safeSkip, safeTake);

                const fetcherPostprocessor = () => {
                    fetcherTask
                        .then((results: pagedResult<T>) => this.chunkFetched(results, safeSkip, safeTake))
                        .fail(error => this.chunkFetchFailed(skip, safeTake))
                        .always(() => {
                            // When we're done loading, run the next queued fetch as necessary.
                            this.isLoading(false);
                            this.runQueuedFetch();
                        });
                };

                if (fetcherTask.state() === "resolved" && !this.checkGridVisibility()) {
                    // look like fetcher works in synchronous mode, but grid is not yet visibile. Use setTimeout, to postpone value provider.
                    // this way we can make sure grid was successfully initialized 
                    setTimeout(() => fetcherPostprocessor(), 0);
                } else {
                    fetcherPostprocessor();
                }
            }
        }
    }

    private makeSkipAndTakeSafe(skip: number, take: number): [number, number] {
        if (this.totalItemCount == null) {
            return [skip, take];
        }

        if (skip > this.totalItemCount) {
            return [0, 0]; // nothing to fetch
        }

        if (skip + take > this.totalItemCount) {
            take = this.totalItemCount - skip;
        }

        // now first first and last missing item in range:
        // [skip, skip + take]
        let firstMissingIdx = null as number;

        for (let i = skip; i < skip + take; i++) {
            if (!this.items.has(i)) {
                firstMissingIdx = i;
                break;
            }
        }

        if (_.isNull(firstMissingIdx)) {
            return [0, 0]; // nothing to take
        }

        const existingItemsDiff = firstMissingIdx - skip;

        return [skip + existingItemsDiff, take - existingItemsDiff];
    }

    private checkForUpdatedGridHeight(): number {
        const windowWidth = window.innerWidth || document.body.clientWidth;
        const windowHeight = window.innerHeight || document.body.clientHeight;
        
        const [prevWidth, prevHeight] = this.previousWindowSize;
        
        if (prevWidth === windowWidth && prevHeight === windowHeight) {
            return this.gridElementHeight;
        }
        
        this.previousWindowSize = [windowWidth, windowHeight];
        
        const oldHeight = this.gridElementHeight;
        const newHeight = this.$gridElement.height();
        this.gridElementHeight = newHeight;

        // If the grid grew taller, we may need more virtual rows.
        if (newHeight > oldHeight) {
            this.initializeVirtualRows();
        }
        
        return newHeight;
    }
    
    private scrollDown() {
        const element = this.$viewportElement[0];
        if (element) {
            element.scrollTop = element.scrollHeight;
        }
    }

    private checkGridVisibility(): boolean {
        // If we've already determined the grid is visible, roll with that.
        if (this.isGridVisible) {
            return true;
        }
        // Grid hasn't yet become visible. Do the more expensive JQuery call.
        else if (this.$gridElement) {
            return (this.isGridVisible = this.$gridElement.is(":visible"));
        }
        return false;
    }

    private runQueuedFetch() {
        if (this.queuedFetch) {
            const { skip, take } = this.queuedFetch;
            this.queuedFetch = null;

            this.fetchItems(skip, take);
        }
    }

    private isSelected(index: number) {
        if (this.inIncludeSelectionMode) {
            return _.includes(this.selectionDiff, index);
        } else {
            return !_.includes(this.selectionDiff, index);
        }
    }

    private layoutVirtualRowPositions() {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.

        // Determine the view port.
        const scrollTop = this.$viewportElement.scrollTop();
        const scrollBottom = scrollTop + this.gridElementHeight;
        let positionCheck = scrollTop;
        const columns = this.columns();
        const lastPossibleRowY = this.virtualHeight() - this.rowHeight; // Find out the last possible row in the grid so that we don't place virtual rows beneath this.

        while (positionCheck < scrollBottom && positionCheck <= lastPossibleRowY) {
            let rowAtPosition = this.findRowAtY(positionCheck);
            if (!rowAtPosition) {
                // There's no row at that spot. Find one we can put there.
                rowAtPosition = this.getOffscreenRow(scrollTop, scrollBottom);

                // Populate it with data.
                const rowIndex = Math.floor(positionCheck / this.rowHeight);
                const isChecked = this.isSelected(rowIndex);
                rowAtPosition.populate(this.items.get(rowIndex), rowIndex, isChecked, columns, columns.indexOf(this.sortByColumn()));
            }

            const newPositionCheck = rowAtPosition.top + this.rowHeight;
            if (newPositionCheck <= positionCheck) {
                throw new Error("Virtual grid defect: next position check was smaller or equal to last check, resulting in potentially infinite loop.");
            }

            positionCheck = newPositionCheck;
        }
    }

    private fillDataIntoRows() {
        // Find which rows are visible.
        const scrollTop = this.$viewportElement.scrollTop();
        const scrollBottom = scrollTop + this.gridElementHeight;
        const columns = this.columns();
        let firstVisibleRowIndex: number | null = null;
        let totalVisible = 0;
        for (let i = 0; i < this.virtualRows.length; i++) {
            const row = this.virtualRows[i];
            const isVisible = !row.isOffscreen(scrollTop, scrollBottom);
            if (isVisible) {
                firstVisibleRowIndex = firstVisibleRowIndex === null ? row.index : Math.min(row.index, firstVisibleRowIndex);
                totalVisible++;

                // Fill it with the data we've got loaded. If there's no data, it will display the loading indicator.
                const isRowChecked = this.isSelected(row.index);
                row.populate(this.items.get(row.index), row.index, isRowChecked, columns, columns.indexOf(this.sortByColumn()));
            }
        }

        // Of the visible rows, are we missing items for any of them?
        let needsToFetch = false;
        if (firstVisibleRowIndex !== null) {
            for (let i = firstVisibleRowIndex; i < firstVisibleRowIndex + totalVisible; i++) {
                if (!this.items.has(i)) {
                    needsToFetch = true;
                    break;
                }
            }
        }

        // We're missing items. Fetch them.
        if (needsToFetch) {
            // Take about 50 items before the first visible index (in case we're scrolling up)
            const skip = Math.max(0, firstVisibleRowIndex - 50);
            const take = Math.max(virtualGrid.minItemFetchCount, totalVisible);
            this.fetchItems(skip, take);
        }
    }

    private findRowAtY(y: number): virtualRow | null {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.
        for (let i = 0; i < this.virtualRows.length; i++) {
            const vRow = this.virtualRows[i];
            const vRowTop = vRow.top;
            const vRowBottom = vRowTop + this.rowHeight;
            if (vRowTop <= y && vRowBottom > y) {
                return vRow;
            }
        }

        return null;
    }

    private getOffscreenRow(viewportTop: number, viewportBottom: number): virtualRow {
        // This is hot path, called multiple times when scrolling.
        // Keep allocations to a minimum.
        for (let i = 0; i < this.virtualRows.length; i++) {
            const row = this.virtualRows[i];
            if (row.isOffscreen(viewportTop, viewportBottom)) {
                return row;
            }
        }

        throw new Error(`Virtual grid defect: couldn't find an offscreen row to recycle. viewportTop = ${viewportTop}, viewportBottom = ${viewportBottom}, recycle row count = ${this.virtualRows.length}`);
    }

    private chunkFetchFailed(skip: number, take: number) {
        // Any rows displaying these items, show an error.
        const endIndex = skip + take;
        const failedRows = this.virtualRows
            .filter(r => !r.hasData && r.index >= skip && r.index <= endIndex);
        failedRows.forEach(r => r.dataLoadError());
    }

    private static percentageToPixels(containerWidth: number, percentageValue: string): string {
        const woPercentage = percentageValue.slice(0, -1);
        return (containerWidth * parseFloat(woPercentage) / 100) + 'px';
    }

    //TODO: investigate if we fetch this properly
    private chunkFetched(results: pagedResult<T>, skip: number, take: number) {

        if (results.totalResultCount === -1) {
            this.emptyResult(true);
            this.virtualHeight(0);
            this.checkForUpdatedGridHeight();
            return;
        }

        if (!this.columns() || this.columns().length === 0) {
            const clientWidth = this.$viewportElement.prop("clientWidth");
            const columns = this.settings.columnsProvider(clientWidth, results);
            columns
                .filter(x => x.width.endsWith("%"))
                .forEach(percentageColumn => {
                    percentageColumn.width = virtualGrid.percentageToPixels(clientWidth, percentageColumn.width);
                });

            this.columns(columns);

            this.syncVirtualWidth();
        }

        this.emptyResult(results.totalResultCount === 0);

        this.updateResultEtag(results.resultEtag);

        // Add these results to the .items array as necessary.
        const oldTotalCount = this.items.size;
        this.totalItemCount = results.totalResultCount;
        this.virtualHeight(results.totalResultCount * this.rowHeight);
        const endIndex = skip + results.items.length;
        for (let i = 0; i < results.items.length; i++) {
            const rowIndex = i + skip;
            if (!this.items.has(rowIndex)) { // newer override existing items, to avoid issues with selected items and jumps
                this.items.set(rowIndex, results.items[i]);
            }
        }
        
        this.sortItems();

        if (oldTotalCount !== results.totalResultCount) {
            this.refreshSelection();
        }

        this.render();
    }

    private updateResultEtag(etag: string) {
        if (etag != null) {
            const previousEtag = this.previousResultsEtag();

            if (previousEtag && previousEtag !== etag) {
                this.dirtyResults(true);
            }

            this.previousResultsEtag(etag);
        }
    }

    private syncHeaderShift() {
        const leftScroll = this.$viewportElement.scrollLeft();
        this.$columnContainer.css({ marginLeft: -leftScroll + 'px' });
    }

    private syncVirtualWidth() {
        if (_.every(this.columns(), x => x.width.endsWith("px"))) {
            const widths = this.columns().map(x => virtualGridUtils.widthToPixels(x));
            this.virtualWidth(_.sum(widths));
        } else {
            // can't auto calculate this
            this.virtualWidth(undefined);
        }
    }

    private gridClicked(e: JQueryEventObject) {
        if (e.target) {
            const $target = this.normalizeTarget($(e.target));
            const actionValue = $target.attr("data-action");
            const linkActionValue = $target.attr("data-link-action");

            if (actionValue) {
                this.handleAction(actionValue, this.findRowForCell($target));
            } else if (linkActionValue) {
                this.handleLinkAction(linkActionValue, e, this.findRowForCell($target));
            } else if ($target.hasClass("checked-column-header")) {
                // If we clicked the the checked column header, toggle select all.
                this.handleSelectAllClicked();
            } else if ($target.hasClass("checked-cell-input")) {
                // If we clicked a checked cell, toggle its selected state.
                const rowIndex = this.findRowForCell($target).index;
                if (rowIndex !== null) {
                    this.toggleRowSelected(rowIndex, e.shiftKey);

                    virtualGridUtils.deselect();
                }
            }
        }
    }

    private normalizeTarget($target: JQuery) {
        const tagName = _.toLower($target.prop("tagName"));
        if (tagName === "label") {
            const input = $target.prev("input");
            if (input) {
                return input;
            }
        } else if (tagName === "span" || tagName === "i") {
            const button = $target.closest("button");
            if (button) {
                return button;
            }
        }
        return $target;
    }

    findRowForCell(cellElement: JQuery | Element): virtualRow {
        return this.virtualRows
            .find(r => r.element.find(cellElement as any).length > 0);
    }

    findColumnForCell(cellElement: Element): virtualColumn {
        const $cell = $(cellElement).closest(".cell");
        const $row = $cell.closest(".virtual-row");
        const $cells = $row.find(".cell");
        const cellIdx = $cells.index($cell);
        return this.columns()[cellIdx];
    }

    /**
     * Clears the items from the grid and fetches again the first chunk of items.
     */
    private resetItems(hard: boolean, retainSort: boolean) {
        if (!this.settings.fetcher) {
            throw new Error("No fetcher defined, call init() method on virtualGridController");
        }
        
        this.items.clear();
        this.totalItemCount = null;
        this.queuedFetch = null;
        this.isLoading(false);
        if (hard) {
            if (retainSort) {
                const sortColumn = this.sortByColumn();
                if (sortColumn) {
                    this.defaultSortByColumn(this.columns().indexOf(sortColumn));
                    this.defaultSortMode(this.sortMode());    
                }
            }

            this.sortMode("asc");
            this.sortByColumn(undefined);
            
            this.$viewportElement.scrollTop(0);
            this.columns([]);
        }
        this.virtualRows.forEach(r => r.reset());
        this.inIncludeSelectionMode = true;
        this.selectionDiff = [];
        this.syncSelectAll();

        this.previousResultsEtag(undefined);
        this.dirtyResults(false);

        this.refreshSelection();

        this.fetchItems(0, 100);
    }

    private refreshSelection(): void {
        const mappedDiff = this.selectionDiff
            .map(idx => this.items.get(idx));

        const selected = this.getSelectionCount();
        const totalCount = this.totalItemCount;

        if (selected > 0 && selected === totalCount) {
            // force exclusive mode - user probably selected all items manually
            this.selection({
                mode: "exclusive",
                included: [],
                excluded: [],
                count: selected,
                totalCount: totalCount
            });
        } else {
            this.selection({
                mode: this.inIncludeSelectionMode ? "inclusive" : "exclusive",
                included: this.inIncludeSelectionMode ? mappedDiff : [],
                excluded: this.inIncludeSelectionMode ? [] : mappedDiff,
                count: selected,
                totalCount: totalCount
            });
        }
    }

    private findItem(predicate: (item: T, idx: number) => boolean): T {
        // since map doesn't guarantee keys order 
        // but we want to simulate that virtual grid holds array
        // let's scan entire cache and get minimum index
        let result: T = undefined;
        let resultIdx = -1;
        
        this.items.forEach((v, i) => {
            if ((resultIdx === -1 || i < resultIdx) && predicate(v, i)) {
                result = v;
                resultIdx = i;
            }
        });
        
        return result;
    }

    private getSelectedItems(): T[] {
        const selection = this.selection();
        if (selection.mode === "inclusive") {
            return this.selection().included;
        } else {
            const excluded = this.selection().excluded;
            if (_.some(this.items, x => !x)) {
                throw new Error("Can't provide list of selected items!");
            }

            const result = [] as T[];
            this.items.forEach(item => {
                if (!_.includes(excluded, item)) {
                    result.push(item);
                }
            });
            return result;
        }
    }

    private setSelectedItems(selection: Array<T>) {
        this.inIncludeSelectionMode = true;
        const selectedIdx = [] as Array<number>;
        this.items.forEach((v, i) => {
           if (_.includes(selection, v)) {
               selectedIdx.push(i);
           } 
        });
        this.selectionDiff = selectedIdx;

        this.syncSelectAll();
        this.refreshSelection();
        this.render();
        this.shiftSelection.lastShiftIndex(null);
    }

    private getSelectionCount() {
        return this.inIncludeSelectionMode ? this.selectionDiff.length : this.totalItemCount - this.selectionDiff.length;
    }

    private toggleRowSelected(rowIndex: number, withShift: boolean) {
        const isSelected = this.isSelected(rowIndex);

        let newShiftStartIndex: number = null;

        const selectUsingRange = withShift && !!this.shiftSelection.selectionRange;

        if (selectUsingRange) {
            const [startIdx, endIdxInclusive] = this.shiftSelection.selectionRange;

            if (this.inIncludeSelectionMode) {
                for (let idx = startIdx; idx <= endIdxInclusive; idx++) {
                    if (!_.includes(this.selectionDiff, idx)) {
                        this.selectionDiff.push(idx);
                    }
                }
            } else {
                for (let idx = startIdx; idx <= endIdxInclusive; idx++) {
                    if (_.includes(this.selectionDiff, idx)) {
                        _.pull(this.selectionDiff, idx);
                    }
                }
            }

        } else {
            if (this.inIncludeSelectionMode) {
                if (isSelected) {
                    _.pull(this.selectionDiff, rowIndex);
                } else {
                    this.selectionDiff.push(rowIndex);
                    newShiftStartIndex = rowIndex;
                }
            } else {
                if (isSelected) {
                    this.selectionDiff.push(rowIndex);
                } else {
                    _.pull(this.selectionDiff, rowIndex);
                    newShiftStartIndex = rowIndex;
                }
            }
        }

        this.syncSelectAll();
        this.refreshSelection();
        this.render();
        this.shiftSelection.lastShiftIndex(newShiftStartIndex);
    }

    private handleAction(actionId: string, row: virtualRow) {
        const handler = this.columns().find(x => x instanceof actionColumn && x.canHandle(actionId)) as actionColumn<T>;
        if (!handler) {
            throw new Error("Unable to find handler for: " + actionId + " at index: " + row.index);
        }

        handler.handle(row);
    }

    private handleLinkAction(actionId: string, event: JQueryEventObject, row: virtualRow) {
        const handler = this.columns().find(x => x instanceof hyperlinkColumn && x.canHandle(actionId)) as hyperlinkColumn<T>;
        if (!handler) {
            throw new Error("Unable to find handler for link action: " + actionId + " at index: " + row.index);
        }

        handler.handle(row, event);
    }

    private handleSelectAllClicked() {
        if (this.getSelectionCount()) {
            // something is selected - deselect all
            this.inIncludeSelectionMode = true;
            this.selectionDiff = [];
        } else {
            // select all
            this.inIncludeSelectionMode = false;
            this.selectionDiff = [];
        }

        this.syncSelectAll();
        this.refreshSelection();
        this.render();
    }

    private syncSelectAll() {
        const $checkboxHeader = $(".checked-column-header", this.$gridElement);

        const selectionCount = this.getSelectionCount();
        if (selectionCount === 0) {
            // none selected
            $checkboxHeader.prop({
                checked: false,
                readonly: false,
                indeterminate: false
            });
        } else if (selectionCount === this.totalItemCount) {
            // all selected
            $checkboxHeader.prop({
                checked: true,
                readonly: false,
                indeterminate: false
            });
        } else {
            $checkboxHeader.prop({
                readonly: true,
                indeterminate: true,
                checked: false
            });
        }
    }

    /**
     * Installs the virtual grid component if it's not already installed.
     */
    static install() {
        const componentName = "virtual-grid";
        if (!ko.components.isRegistered(componentName)) {
            ko.components.register(componentName, {
                viewModel: virtualGrid,
                template: `
<div class="virtual-grid flex-window stretch" data-bind="attr: { id: gridId }, css: { condensed : condensed }">
    <div class="absolute-center loading" data-bind="visible: isLoading"><div class="global-spinner"></div></div>
    <div class="column-container flex-window-head" data-bind="foreach: columns, visible: settings.showHeader"><div class="column" data-bind="style: { width: $data.width }, attr: { title: $data.headerTitle }, css: { sortable: sortable, asc: $data === $parent.sortByColumn() && $parent.sortMode() === 'asc', desc: $data === $parent.sortByColumn() && $parent.sortMode() === 'desc' }"><div class="sortable-controls"></div><strong data-bind="html: $data.header"></strong></div></div>    
    <div class="viewport flex-window-scroll" data-bind="css: { 'header-visible': settings.showHeader }">
        <div class="viewport-scroller" data-bind="style: { height: virtualHeight() + 'px', width: virtualWidth() + 'px' }, template: { afterRender: afterRender.bind($data) }">
        </div>
    </div>
    <div class="absolute-center" data-bind="visible: !isLoading() && emptyTemplate && emptyResult(), if: emptyTemplate">
        <div data-bind="template: emptyTemplate"></div>
    </div>
</div>
`
            });
        }
    }
}

export = virtualGrid;
