import router = require("plugins/router");
import widget = require("plugins/widget");
import app = require("durandal/app");

import pagedList = require("common/pagedList");
import appUrl = require("common/appUrl");
import document = require("models/document");
import collection = require("models/collection");
import database = require("models/database");
import pagedResultSet = require("common/pagedResultSet");
import deleteItems = require("viewmodels/deleteItems");
import copyDocuments = require("viewmodels/copyDocuments");
import row = require("widgets/virtualTable/row");
import column = require("widgets/virtualTable/column");
import customColumnParams = require('models/customColumnParams');
import customColumns = require('models/customColumns');

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
    itemsSourceSubscription: KnockoutSubscription = null;
    isIndexMapReduce: KnockoutObservable<boolean>;

    settings: {
        itemsSource: KnockoutObservable<pagedList>;
        dynamicHeightTargetSelector: string;
        dynamicHeightBottomMargin: number;
        gridSelector: string;
        selectedIndices: KnockoutObservableArray<number>;
        showCheckboxes: boolean;
        showIds: boolean;
        useContextMenu: boolean;
        maxHeight: string;
        customColumnParams: { [column: string]: customColumnParams };
        isIndexMapReduce: KnockoutObservable<boolean>;
        isCopyAllowed: boolean;
        contextMenuOptions: string[];
        selectionEnabled: boolean;
        customColumns: KnockoutObservable<customColumns>;
    }

    activate(settings: any) {
        var defaults = {
            dynamicHeightTargetSelector: "footer",
            dynamicHeightBottomMargin: 0,
            selectedIndices: ko.observableArray(),
            showCheckboxes: true,
            showIds: true,
            useContextMenu: true,
            maxHeight: 'none',
            customColumnParams: {},
            isIndexMapReduce: ko.observable<boolean>(true),
            isCopyAllowed: true,
            contextMenuOptions: ["CopyItems", "CopyIDs", "Delete"],
            selectionEnabled: true,
            customColumns: ko.observable(customColumns.empty())
        };
        this.settings = $.extend(defaults, settings);


        if (!!settings.isIndexMapReduce) {
            this.isIndexMapReduce = settings.isIndexMapReduce;
        } else {
            this.isIndexMapReduce = ko.observable<boolean>(false);
        }

        this.items = this.settings.itemsSource();
        this.focusableGridSelector = this.settings.gridSelector + " .ko-grid";
        this.virtualHeight = ko.computed(() => this.rowHeight * this.virtualRowCount());

        this.refreshIdAndCheckboxColumn();

        this.itemsSourceSubscription = this.settings.itemsSource.subscribe(list => {
            this.recycleRows().forEach(r => {
                r.resetCells();
                this.recycleRows.valueHasMutated();
                this.columns.valueHasMutated();
                r.isInUse(false);
            });
            this.items = list;
            this.settings.selectedIndices.removeAll();
            this.columns.remove(c => (c.binding !== 'Id' && c.binding !== '__IsChecked'));
            this.gridViewport.scrollTop(0);
            this.onGridScrolled();

            this.refreshIdAndCheckboxColumn();
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
        if (this.itemsSourceSubscription) {
            this.itemsSourceSubscription.dispose();
        }
    }

    calculateRecycleRowCount() {
        var requiredRowCount = Math.ceil(this.viewportHeight() / this.rowHeight);
        var rowCountWithPadding = requiredRowCount + 10;
        return rowCountWithPadding;
    }

    createRecycleRows(rowCount: number) {
        var rows = [];
        for (var i = 0; i < rowCount; i++) {
            var newRow = new row(this.settings.showIds, this);
            newRow.createPlaceholderCells(this.columns().map(c => c.binding));
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
        this.setupKeyboardShortcut("DELETE", () => this.deleteSelectedItems());
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

                if (this.settings.showCheckboxes == true && !this.isIndexMapReduce()) {
                    // Select any right-clicked row.
                    var parentRow = $(e.target).parent(".ko-grid-row");
                    var rightClickedElement: row = parentRow.length ? ko.dataFor(parentRow[0]) : null;
                    if (rightClickedElement && rightClickedElement.isChecked != null && !rightClickedElement.isChecked()) {
                        this.toggleRowChecked(rightClickedElement, e.shiftKey);
                    }
                }
                return true;
            }
        });
    }

    refreshIdAndCheckboxColumn() {
        var containsId = this.columns().first(x=> x.binding == "Id");

        if (!containsId && !this.isIndexMapReduce()) {
            if (this.settings.showCheckboxes !== false) {
                this.columns.push(new column("__IsChecked", 38));
            }
            if (this.settings.showIds !== false) {
                this.columns.push(new column("Id", ctor.idColumnWidth));
            }
            this.columns.valueHasMutated();
        } else if (containsId && this.isIndexMapReduce()) {
            this.columns.remove(c => c.binding === 'Id' || c.binding === "__IsChecked");
            this.columns.valueHasMutated();
        }
    }

    loadRowData() {
        if (this.items && this.firstVisibleRow) {
            var that = this;
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

    fillRow(rowData: documentBase, rowIndex: number) {
        var rowAtIndex: row = ko.utils.arrayFirst(this.recycleRows(), (r: row) => r.rowIndex() === rowIndex);
        if (rowAtIndex) {
            rowAtIndex.fillCells(rowData);
            rowAtIndex.collectionClass(this.getCollectionClassFromDocument(rowData));
            rowAtIndex.editUrl(appUrl.forEditItem(rowData.getId(), appUrl.getResource(), rowIndex, this.getEntityName(rowData)));
        }
    }

    editLastSelectedItem() {
        var selectedItem = this.getSelectedItems(1).first();
        if (selectedItem) {
            var collectionName = this.items.collectionName;
            var itemIndex = this.settings.selectedIndices().first();
            router.navigate(appUrl.forEditItem(selectedItem.getId(), appUrl.getResource(), itemIndex, collectionName));
        }
    }

    getEntityName(item: documentBase) {
        var obj: any = item;
        if (obj && obj instanceof document && obj.getEntityName) {
            var documentObj = <document> obj;
            return documentObj.getEntityName();
        }
        return null;
    }

    getCollectionClassFromDocument(doc: documentBase): string {
        return collection.getCollectionCssClass(this.getEntityName(doc));
    }

    getColumnWidth(binding: string, defaultColumnWidth: number = 100): number {
        var customColumns = this.settings.customColumns();
        var customConfig = customColumns.findConfigFor(binding);
        if (customConfig && customColumns.customMode() === true) {
            return customConfig.width();
        }

        if (binding === "Id" && defaultColumnWidth > ctor.idColumnWidth) {
            return ctor.idColumnWidth;
        }
        return defaultColumnWidth;
    }

    getColumnName(binding: string): string {
        if (this.settings.customColumns().hasOverrides()) {
            var customConfig = this.settings.customColumns().findConfigFor(binding);
            if (customConfig) {
                return customConfig.header();
            }
        }
        return binding;
    }

    ensureColumnsForRows(rows: Array<documentBase>) {
        // This is called when items finish loading and are ready for display.
        // Keep allocations to a minimum.

        var columnsNeeded = {};
        
        if (this.settings.customColumns().hasOverrides()) {
            var colParams = this.settings.customColumns().columns();
            for (var i = 0; i < colParams.length; i++) {
                var colParam = colParams[i];
                columnsNeeded[colParam.binding()] = null;
            }
        } else {
            for (var i = 0; i < rows.length; i++) {
                var currentRow = rows[i];
                var rowProperties = currentRow.getDocumentPropertyNames();
                for (var j = 0; j < rowProperties.length; j++) {
                    var property = rowProperties[j];
                    columnsNeeded[property] = null;
                }
            }
        }

        for (var i = 0; i < this.columns().length; i++) {
            var colName = this.columns()[i].binding;
            delete columnsNeeded[colName];
        }

        var idColumn = this.columns.first(x=> x.binding == "Id");
        var idColumnExists = idColumn ? 1 : 0;

        var unneededColumns = new Array<string>();
        ko.utils.arrayForEach(this.columns(), col => {
            if (col.binding !== "Id" && col.binding !== "__IsChecked" &&
                rows.every(row => !row.getDocumentPropertyNames().contains(col.binding)))
                unneededColumns.push(col.binding);
        });

        this.columns.remove(c => unneededColumns.contains(c.binding));
        this.columns.valueHasMutated();
        this.settings.customColumns().columns.remove(c => unneededColumns.contains(c.binding()));

        var columnsCurrentTotalWidth = 0;
        for (var i = 2; i < this.columns().length; i++) {
            columnsCurrentTotalWidth += this.columns()[i].width();
        }

        var availiableWidth = this.grid.width() - 200 * idColumnExists - columnsCurrentTotalWidth;
        var freeWidth = availiableWidth;
        var fontSize = parseInt(this.grid.css("font-size"));
        var columnCount = 0;
        for (var binding in columnsNeeded) {
            var curColWidth = (binding.length + 2) * fontSize;
            if (freeWidth - curColWidth < 0) {
                break;
            }
            freeWidth -= curColWidth;
            columnCount++;
        }
        var freeWidthPerColumn = (freeWidth / (columnCount + 1));

        var firstRow = this.recycleRows().length > 0 ? this.recycleRows()[0] : null;
        for (var binding in columnsNeeded) {
            var curColWidth = (binding.length + 2) * fontSize + freeWidthPerColumn;
            var columnWidth = this.getColumnWidth(binding, curColWidth);
            availiableWidth -= columnWidth;
            if (availiableWidth <= 0) {
                break;
            }
            var columnName = this.getColumnName(binding);

            // Give priority to any Name column. Put it after the check column (0) and Id (1) columns.
            var newColumn = new column(binding, columnWidth, columnName);
            if ((binding === "Name") && (!this.settings.customColumns().customMode())){
                this.columns.splice(2, 0, newColumn);
            } else {
                this.columns.push(newColumn);
            }

            var curColumnConfig = this.settings.customColumns().findConfigFor(binding);
            if (!curColumnConfig && !!firstRow) {
                var curColumnTemplate: string = firstRow.getCellTemplate(binding);
                var newCustomColumn = new customColumnParams({
                    Binding: binding,
                    Header: binding,
                    Template: curColumnTemplate,
                    DefaultWidth: availiableWidth > 0 ? Math.floor(columnWidth) : 0
                });
                if ((binding === "Name") && (!this.settings.customColumns().customMode())) {
                    this.settings.customColumns().columns.splice(0, 0, newCustomColumn);
                } else {
                    this.settings.customColumns().columns.push(newCustomColumn);
                }
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

    getTemplateFor(columnName: string): string {
        if (this.settings.customColumns().hasOverrides()) {
            var customConfig = this.settings.customColumns().findConfigFor(columnName);
            if (customConfig) {
                return customConfig.template();
            }
        }
        return undefined;
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
        var selectedDocs = this.getSelectedItems();
        var copyDocumentsVm = new copyDocuments(selectedDocs, this.focusableGridSelector);
        copyDocumentsVm.isCopyingDocs(idsOnly === false);
        app.showDialog(copyDocumentsVm);
    }

    getSelectedItems(max?: number): Array<any> {
        if (!this.items || this.settings.selectedIndices().length === 0) {
            return [];
        }
        var sliced = max ? <number[]>this.settings.selectedIndices.slice(0, max) : null;
        var maxSelectedIndices = sliced || <number[]>this.settings.selectedIndices();
        return this.items.getCachedItemsAt(maxSelectedIndices);
    }

    deleteSelectedItems() {
        var documents = this.getSelectedItems();
        var deleteDocsVm = new deleteItems(documents, this.focusableGridSelector);
        var self = this;
        deleteDocsVm.deletionTask.done(() => {
            var deletedDocIndices = documents.map(d => self.items.indexOf(d));
            deletedDocIndices.forEach(i => self.settings.selectedIndices.remove(i));
            self.recycleRows().forEach(r => r.isChecked(self.settings.selectedIndices().contains(r.rowIndex()))); // Update row checked states.
            self.items.invalidateCache(); // Causes the cache of items to be discarded.
            self.onGridScrolled(); // Forces a re-fetch of the rows in view.
        });

        app.showDialog(deleteDocsVm);
    }

    getDocumentHref(documentId): string {
        if (typeof documentId == "string") {
            return appUrl.forEditItem(documentId, appUrl.getDatabase(), null, null);
        } else {
            return "#";
        }
    }
}

export = ctor;

