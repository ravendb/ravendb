import router = require("plugins/router");
import widget = require("plugins/widget");
import app = require("durandal/app");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import document = require("models/document");
import collection = require("models/collection");
import database = require("models/database");
import pagedResultSet = require("common/pagedResultSet"); 
import deleteDocuments = require("viewmodels/deleteDocuments");
import copyDocuments = require("viewmodels/copyDocuments");
import row = require("widgets/virtualTable/row");
import column = require("widgets/virtualTable/column");

class ctor {

    static idColumnWidth = 200;

    items: pagedList;
    visibleRowCount = 0;
    recycleRows = ko.observableArray<row>();
    rowHeight = 38;
    borderHeight = 2;
    virtualHeight: KnockoutComputed<number>;
    viewportHeight = ko.observable(0);
    virtualRowCount = ko.observable(0);
    grid: JQuery;
    focusableGridSelector: string;
    columns = ko.observableArray<column>();
    gridViewport: JQuery;
    scrollThrottleTimeoutHandle = 0;
    firstVisibleRow: row = null;

    settings: {
        documentsSource: KnockoutObservable<pagedList>;
        dynamicHeightTargetSelector: string;
        dynamicHeightBottomMargin: number;
        gridSelector: string;
        selectedIndices: KnockoutObservableArray<number>;
        showCheckboxes: boolean;
        showIds: boolean;
        useContextMenu: boolean;
        maxHeight: string;
    }

    constructor() {
    }

    activate(settings: any) {
        var defaults = {
            dynamicHeightTargetSelector: "footer",
            dynamicHeightBottomMargin: 0,
            selectedIndices: ko.observableArray(),
            showCheckboxes: true,
            showIds: true,
            useContextMenu: true,
            maxHeight: 'none'
        };
        this.settings = $.extend(defaults, settings);

        this.items = this.settings.documentsSource();
        this.focusableGridSelector = this.settings.gridSelector + " .ko-grid";
        this.virtualHeight = ko.computed(() => this.rowHeight * this.virtualRowCount());
        if (this.settings.showCheckboxes !== false) {
            this.columns.push(new column("__IsChecked", 38));
        }
        if (this.settings.showIds !== false) {
            this.columns.push(new column("Id", ctor.idColumnWidth));
        }

        this.settings.documentsSource.subscribe(list => {
            this.recycleRows().forEach(r => {
                r.resetCells();
                r.isInUse(false);
            });
            this.items = list;
            this.settings.selectedIndices.removeAll();
            this.columns.remove(c => (c.name !== 'Id' && c.name !== '__IsChecked'));
            this.gridViewport.scrollTop(0);
            this.onGridScrolled();
        });
    }

    // Attached is called by Durandal when the view is attached to the DOM.
    // We use this to setup some UI-specific things like context menus, row creation, keyboard shortcuts, etc.
    attached() {
        this.grid = $(this.settings.gridSelector);
        if (this.grid.length !== 1) {
            throw new Error("There should be 1 " + this.settings.gridSelector + " on the page, but found " + this.grid.length.toString());
        }

        this.gridViewport = this.grid.find(".ko-grid-viewport-container");
        this.gridViewport.on('DynamicHeightSet', () => this.onWindowHeightChanged());
        this.gridViewport.scroll(() => this.onGridScrolled());
        this.setupKeyboardShortcuts();
        if (this.settings.useContextMenu) {
            this.setupContextMenu();
        }
    }

    detached() {
        $(this.settings.gridSelector).unbind('keydown.jwerty');
        this.gridViewport.off('DynamicHeightSet');
    }

    calculateRecycleRowCount() {
        var requiredRowCount = Math.ceil(this.viewportHeight() / this.rowHeight);
        var rowCountWithPadding = requiredRowCount + 10;
        return rowCountWithPadding;
    }

    createRecycleRows(rowCount: number) {
        var rows = [];
        for (var i = 0; i < rowCount; i++) {
            var newRow = new row(this.settings.showIds);
            newRow.createPlaceholderCells(this.columns().map(c => c.name));
            newRow.rowIndex(i);
            var desiredTop = i * this.rowHeight;
            newRow.top(desiredTop);
            rows.push(newRow);
        }

        return rows;
    }

    onGridScrolled() {
        this.ensureRowsCoverViewport();

        window.clearTimeout(this.scrollThrottleTimeoutHandle);
        this.scrollThrottleTimeoutHandle = setTimeout(() => this.loadRowData(), 100);
        
        // COMMENTED OUT: while requestAnimationFrame works, there are some problems:
        // 1. It needs polyfill on IE9 and earlier.
        // 2. While the screen redraws much faster, it results in a more laggy scroll.
        //window.cancelAnimationFrame(this.scrollThrottleTimeoutHandle);
        //this.scrollThrottleTimeoutHandle = window.requestAnimationFrame(() => this.loadRowData());
    }

    onWindowHeightChanged() {
        var newViewportHeight = this.gridViewport.height();
        this.viewportHeight(newViewportHeight);
        var desiredRowCount = this.calculateRecycleRowCount();
        this.recycleRows(this.createRecycleRows(desiredRowCount));
        this.ensureRowsCoverViewport();
        this.loadRowData();        
    }

    setupKeyboardShortcuts() {
        this.setupKeyboardShortcut("DELETE", () => this.deleteSelectedDocs());
        this.setupKeyboardShortcut("Ctrl+C,D", () => this.copySelectedDocs());
        this.setupKeyboardShortcut("Ctrl+C,I", () => this.copySelectedDocIds());
    }

    setupKeyboardShortcut(keys: string, handler: () => void) {
        jwerty.key(keys, e => {
            e.preventDefault();
            handler();
        }, this, this.settings.gridSelector);
    }

    setupContextMenu() {
        var untypedGrid: any = this.grid;
        untypedGrid.contextmenu({
            target: '#gridContextMenu',
            before: (e: MouseEvent) => { 

                // Select any right-clicked row.
                var parentRow = $(e.target).parent(".ko-grid-row");
                var rightClickedElement: row = parentRow.length ? ko.dataFor(parentRow[0]) : null;
                if (rightClickedElement && rightClickedElement.isChecked != null && !rightClickedElement.isChecked()) {
                    this.toggleRowChecked(rightClickedElement, e.shiftKey);
                }

                return true;
            }
        });
    }

    loadRowData() {
        if (this.items && this.firstVisibleRow) {
            // The scrolling has paused for a minute. See if we have all the data needed.
            var firstVisibleIndex = this.firstVisibleRow.rowIndex();
            var fetchTask = this.items.fetch(firstVisibleIndex, this.recycleRows().length);
            fetchTask.done((resultSet: pagedResultSet) => {
                var firstVisibleRowIndexHasChanged = firstVisibleIndex !== this.firstVisibleRow.rowIndex();
                if (!firstVisibleRowIndexHasChanged) {
                    this.virtualRowCount(resultSet.totalResultCount);
                    resultSet.items.forEach((r, i) => this.fillRow(r, i + firstVisibleIndex));
                    this.ensureColumnsForRows(resultSet.items);
                }
            });
        }
    }

    fillRow(rowData: document, rowIndex: number) {
        var rowAtIndex: row = ko.utils.arrayFirst(this.recycleRows(), (r: row) => r.rowIndex() === rowIndex);
        if (rowAtIndex) {
            rowAtIndex.fillCells(rowData);
            rowAtIndex.collectionClass(this.getCollectionClassFromDocument(rowData));
            rowAtIndex.editUrl(appUrl.forEditDoc(rowData.getId(), rowData.__metadata.ravenEntityName, rowIndex, appUrl.getDatabase()));
        }
    }

    editLastSelectedDoc() {
        var selectedDoc = this.getSelectedDocs(1).first();
        if (selectedDoc) {
            var id = selectedDoc.getId();
            var collectionName = this.items.collectionName;
            var itemIndex = this.settings.selectedIndices().first();
            router.navigate(appUrl.forEditDoc(id, collectionName, itemIndex, appUrl.getDatabase()));
        }
    }

    getCollectionClassFromDocument(doc: document): string {
        var entityName = doc.__metadata.ravenEntityName;
        return collection.getCollectionCssClass(entityName);
    }

    ensureColumnsForRows(rows: Array<document>) {
        // This is called when items finish loading and are ready for display.
        // Keep allocations to a minimum.

        // Enforce a max number of columns. Having many columns is unweildy to the user
        // and greatly slows down scroll speed.
        var maxColumns = 10;
        if (this.columns().length >= maxColumns) {
            return;
        }

        var columnsNeeded = {};
        for (var i = 0; i < rows.length; i++) {
            var currentRow = rows[i];
            var rowProperties = currentRow.getDocumentPropertyNames();
            for (var j = 0; j < rowProperties.length; j++) {
                var property = rowProperties[j];
                columnsNeeded[property] = null;
            }
        }

        for (var i = 0; i < this.columns().length; i++) {
            var colName = this.columns()[i].name;
            delete columnsNeeded[colName];
        }

        for (var prop in columnsNeeded) {
            var defaultColumnWidth = 200;
            var columnWidth = defaultColumnWidth;
            if (prop === "Id") {
                columnWidth = ctor.idColumnWidth;
            }

            // Give priority to any Name column. Put it after the check column (0) and Id (1) columns.
            var newColumn = new column(prop, columnWidth);
            if (prop === "Name") {
                this.columns.splice(2, 0, newColumn);
            } else if (this.columns().length < 10) {
                this.columns.push(newColumn);
            }
        }
    }

    ensureRowsCoverViewport() {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.
        var viewportTop = this.gridViewport.scrollTop();
        var viewportBottom = viewportTop + this.viewportHeight();
        var positionCheck = viewportTop;

        this.firstVisibleRow = null;
        while (positionCheck < viewportBottom) {
            var rowAtPosition = this.findRowAtY(positionCheck);
            if (!rowAtPosition) {
                // If there's no row at this position, recycle one.
                rowAtPosition = this.getOffscreenRow(viewportTop, viewportBottom);

                // Find out what the new top of the row should be.
                var rowIndex = Math.floor(positionCheck / this.rowHeight);
                var desiredNewRowY = rowIndex * this.rowHeight;
                rowAtPosition.top(desiredNewRowY);
                rowAtPosition.rowIndex(rowIndex);
                rowAtPosition.resetCells();
                rowAtPosition.isChecked(this.settings.selectedIndices.indexOf(rowIndex) !== -1);
            }

            if (!this.firstVisibleRow) {
                this.firstVisibleRow = rowAtPosition;
            }

            positionCheck = rowAtPosition.top() + this.rowHeight;
        }
    }

    getOffscreenRow(viewportTop: number, viewportBottom: number) {
        // This is hot path, called multiple times when scrolling.
        // Keep allocations to a minimum.
        var rows = this.recycleRows();
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var rowTop = row.top();
            var rowBottom = rowTop + this.rowHeight;
            if (rowTop > viewportBottom || rowBottom < viewportTop) {
                return row;
            }
        }

        throw new Error("Bug: couldn't find an offscreen row to recycle. viewportTop = " + viewportTop.toString() + ", viewportBottom = " + viewportBottom.toString() + ", recycle row count = " + rows.length.toString());
    }

    findRowAtY(y: number) {
        // This is hot path, called multiple times when scrolling. 
        // Keep allocations to a minimum.
        var rows = this.recycleRows();
        for (var i = 0; i < rows.length; i++) {
            var row = rows[i];
            var rowTop = row.top();
            var rowBottom = rowTop + this.rowHeight;
            if (rowTop <= y && rowBottom > y) {
                return row;
            }
        }

        return null;
    }

    toggleRowChecked(row: row, isShiftSelect = false) {
        var rowIndex = row.rowIndex();
        var isChecked = row.isChecked();
        var firstIndex = <number>this.settings.selectedIndices.first();
        var toggledIndices: Array<number> = isShiftSelect && this.settings.selectedIndices().length > 0 ? this.getRowIndicesRange(firstIndex, rowIndex) : [rowIndex];
        if (!isChecked) {
            // Going from unchecked to checked.
            if (this.settings.selectedIndices.indexOf(rowIndex) === -1) {
                toggledIndices
                    .filter(i => !this.settings.selectedIndices.contains(i))
                    .reverse()
                    .forEach(i => this.settings.selectedIndices.unshift(i));
            }
        } else {
            // Going from checked to unchecked.
            this.settings.selectedIndices.removeAll(toggledIndices);
        }

        this.recycleRows().forEach(r => r.isChecked(this.settings.selectedIndices().contains(r.rowIndex())));
    }

    selectNone() {
        this.settings.selectedIndices([]);
        this.recycleRows().forEach(r => r.isChecked(false));
    }

    selectAll() {
        var allIndices = [];
        for (var i = 0; i < this.items.totalResultCount(); i++) {
            allIndices.push(i);
        }
        this.settings.selectedIndices(allIndices);
        this.recycleRows().forEach(r => r.isChecked(true));
    }

    getRowIndicesRange(firstRowIndex: number, secondRowIndex: number): Array<number> {
        var isCountingDown = firstRowIndex > secondRowIndex;
        var indices: Array<number> = [];
        if (isCountingDown) {
            for (var i = firstRowIndex; i >= secondRowIndex; i--) indices.unshift(i);
        } else {
            for (var i = firstRowIndex; i <= secondRowIndex; i++) indices.unshift(i);
        }

        return indices;
    }

    copySelectedDocs() {
        this.showCopyDocDialog(false);
    }

    copySelectedDocIds() {
        this.showCopyDocDialog(true);
    }

    showCopyDocDialog(idsOnly: boolean) {
        var selectedDocs = this.getSelectedDocs();
        var copyDocumentsVm = new copyDocuments(selectedDocs, this.focusableGridSelector);
        copyDocumentsVm.isCopyingDocs(idsOnly === false);
        app.showDialog(copyDocumentsVm);
    }

    getSelectedDocs(max?: number): Array<document> {
        if (!this.items || this.settings.selectedIndices().length === 0) {
            return [];
        }
        var sliced = max ? <number[]>this.settings.selectedIndices.slice(0, max) : null;
        var maxSelectedIndices = sliced || <number[]>this.settings.selectedIndices();
        return this.items.getCachedItemsAt(maxSelectedIndices);
    }

    deleteSelectedDocs() {
        var documents = this.getSelectedDocs();
        var deleteDocsVm = new deleteDocuments(documents, this.focusableGridSelector);
        deleteDocsVm.deletionTask.done(() => {
            var deletedDocIndices = documents.map(d => this.items.indexOf(d));
            deletedDocIndices.forEach(i => this.settings.selectedIndices.remove(i));
            this.recycleRows().forEach(r => r.isChecked(this.settings.selectedIndices().contains(r.rowIndex()))); // Update row checked states.
            this.items.invalidateCache(); // Causes the cache of items to be discarded.
            this.onGridScrolled(); // Forces a re-fetch of the rows in view.
        });

        app.showDialog(deleteDocsVm);
    }
}

export = ctor;
