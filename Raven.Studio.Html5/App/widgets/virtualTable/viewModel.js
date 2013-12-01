/// <reference path="../../../Scripts/typings/knockout.postbox/knockout-postbox.d.ts" />
/// <reference path="../../../Scripts/typings/durandal/durandal.d.ts" />
define(["require", "exports", "common/pagedList", "common/raven", "common/appUrl", "models/document", "models/collection", "models/database", "common/pagedResultSet", "viewmodels/deleteDocuments", "viewmodels/copyDocuments", "durandal/app", "widgets/virtualTable/row", "widgets/virtualTable/column"], function(require, exports, __pagedList__, __raven__, __appUrl__, __document__, __collection__, __database__, __pagedResultSet__, __deleteDocuments__, __copyDocuments__, __app__, __row__, __column__) {
    
    var pagedList = __pagedList__;
    var raven = __raven__;
    var appUrl = __appUrl__;
    var document = __document__;
    var collection = __collection__;
    var database = __database__;
    var pagedResultSet = __pagedResultSet__;
    var deleteDocuments = __deleteDocuments__;
    var copyDocuments = __copyDocuments__;
    var app = __app__;
    var row = __row__;
    var column = __column__;

    var ctor = (function () {
        function ctor() {
            this.visibleRowCount = 0;
            this.recycleRows = ko.observableArray();
            this.rowHeight = 38;
            this.borderHeight = 2;
            this.viewportHeight = ko.observable(0);
            this.virtualRowCount = ko.observable(0);
            this.columns = ko.observableArray([
                new column("__IsChecked", 38),
                new column("Id", ctor.idColumnWidth)
            ]);
            this.scrollThrottleTimeoutHandle = 0;
            this.firstVisibleRow = null;
            this.selectedIndices = ko.observableArray();
        }
        ctor.prototype.activate = function (settings) {
            var _this = this;
            var docsSource = settings.documentsSource;
            docsSource.subscribe(function (list) {
                _this.recycleRows().forEach(function (r) {
                    r.resetCells();
                    r.isInUse(false);
                });
                _this.items = list;
                _this.selectedIndices.removeAll();
                _this.columns.splice(2, _this.columns().length - 1);
                _this.onGridScrolled();
            });

            this.items = docsSource();
            this.collections = settings.collections;
            this.viewportHeight(settings.height);
            this.gridSelector = settings.gridSelector;
            this.virtualHeight = ko.computed(function () {
                return _this.rowHeight * _this.virtualRowCount();
            });
        };

        // Attached is called by Durandal when the view is attached to the DOM.
        // We use this to setup some UI-specific things like context menus, row creation, keyboard shortcuts, etc.
        ctor.prototype.attached = function () {
            var _this = this;
            this.grid = $(this.gridSelector);
            if (this.grid.length !== 1) {
                throw new Error("There should be 1 " + this.gridSelector + " on the page, but found " + this.grid.length.toString());
            }

            this.gridViewport = this.grid.find(".ko-grid-viewport-container");
            this.gridViewport.scroll(function () {
                return _this.onGridScrolled();
            });
            var desiredRowCount = this.calculateRecycleRowCount();
            this.recycleRows(this.createRecycleRows(desiredRowCount));
            this.ensureRowsCoverViewport();
            this.loadRowData();
            this.setupContextMenu();
            this.setupKeyboardShortcuts();
        };

        ctor.prototype.detached = function () {
            $(this.gridSelector).unbind('keydown.jwerty');
        };

        ctor.prototype.calculateRecycleRowCount = function () {
            var requiredRowCount = Math.ceil(this.viewportHeight() / this.rowHeight);
            var rowCountWithPadding = requiredRowCount + 10;
            return rowCountWithPadding;
        };

        ctor.prototype.createRecycleRows = function (rowCount) {
            var rows = [];
            for (var i = 0; i < rowCount; i++) {
                var newRow = new row();
                newRow.rowIndex(i);
                var desiredTop = i * this.rowHeight;
                newRow.top(desiredTop);
                rows.push(newRow);
            }

            return rows;
        };

        ctor.prototype.onGridScrolled = function () {
            var _this = this;
            this.ensureRowsCoverViewport();

            window.clearTimeout(this.scrollThrottleTimeoutHandle);
            this.scrollThrottleTimeoutHandle = setTimeout(function () {
                return _this.loadRowData();
            });
        };

        ctor.prototype.setupKeyboardShortcuts = function () {
            var _this = this;
            jwerty.key("delete", function (e) {
                e.preventDefault();
                _this.deleteSelectedDocs();
            }, this, this.gridSelector);
        };

        ctor.prototype.setupContextMenu = function () {
            var _this = this;
            var untypedGrid = this.grid;
            untypedGrid.contextmenu({
                target: '#gridContextMenu',
                before: function (e) {
                    // Select any right-clicked row.
                    var parentRow = $(e.target).parent(".ko-grid-row");
                    var rightClickedElement = parentRow.length ? ko.dataFor(parentRow[0]) : null;
                    if (rightClickedElement && rightClickedElement.isChecked != null && !rightClickedElement.isChecked()) {
                        _this.toggleRowChecked(rightClickedElement, e.shiftKey);
                    }

                    return true;
                }
            });
        };

        ctor.prototype.loadRowData = function () {
            var _this = this;
            // The scrolling has paused for a minute. See if we have all the data needed.
            var firstVisibleIndex = this.firstVisibleRow.rowIndex();
            var fetchTask = this.items.fetch(firstVisibleIndex, this.recycleRows().length);
            fetchTask.done(function (resultSet) {
                var firstVisibleRowIndexHasChanged = firstVisibleIndex !== _this.firstVisibleRow.rowIndex();
                if (!firstVisibleRowIndexHasChanged) {
                    _this.virtualRowCount(resultSet.totalResultCount);
                    resultSet.items.forEach(function (r, i) {
                        return _this.fillRow(r, i + firstVisibleIndex);
                    });
                    _this.ensureColumnsForRows(resultSet.items);
                }
            });
        };

        ctor.prototype.fillRow = function (rowData, rowIndex) {
            var rowAtIndex = ko.utils.arrayFirst(this.recycleRows(), function (r) {
                return r.rowIndex() === rowIndex;
            });
            if (rowAtIndex) {
                rowAtIndex.fillCells(rowData);
                rowAtIndex.collectionClass(this.getCollectionClassFromDocument(rowData));
                rowAtIndex.editUrl(appUrl.forEditDoc(rowData.getId(), rowData.__metadata.ravenEntityName, rowIndex));
            }
        };

        ctor.prototype.getCollectionClassFromDocument = function (doc) {
            var collectionName = doc.__metadata.ravenEntityName;
            var collection = this.collections().first(function (c) {
                return c.name === collectionName;
            });
            if (collection) {
                return collection.colorClass;
            }

            return null;
        };

        ctor.prototype.ensureColumnsForRows = function (rows) {
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
        };

        ctor.prototype.ensureRowsCoverViewport = function () {
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
                    rowAtPosition.isChecked(this.selectedIndices.indexOf(rowIndex) !== -1);
                }

                if (!this.firstVisibleRow) {
                    this.firstVisibleRow = rowAtPosition;
                }

                positionCheck = rowAtPosition.top() + this.rowHeight;
            }
        };

        ctor.prototype.getOffscreenRow = function (viewportTop, viewportBottom) {
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
        };

        ctor.prototype.findRowAtY = function (y) {
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
        };

        ctor.prototype.toggleRowChecked = function (row, isShiftSelect) {
            if (typeof isShiftSelect === "undefined") { isShiftSelect = false; }
            var _this = this;
            var rowIndex = row.rowIndex();
            var isChecked = row.isChecked();
            var toggledIndices = isShiftSelect && this.selectedIndices().length > 0 ? this.getRowIndicesRange(this.selectedIndices.first(), rowIndex) : [rowIndex];
            if (!isChecked) {
                if (this.selectedIndices.indexOf(rowIndex) === -1) {
                    toggledIndices.filter(function (i) {
                        return !_this.selectedIndices.contains(i);
                    }).reverse().forEach(function (i) {
                        return _this.selectedIndices.unshift(i);
                    });
                }
            } else {
                // Going from checked to unchecked.
                this.selectedIndices.removeAll(toggledIndices);
            }

            // Update the physical checked state of the rows.
            this.recycleRows().forEach(function (r) {
                return r.isChecked(_this.selectedIndices().indexOf(r.rowIndex()) !== -1);
            });
        };

        ctor.prototype.getRowIndicesRange = function (firstRowIndex, secondRowIndex) {
            var isCountingDown = firstRowIndex > secondRowIndex;
            var indices = [];
            if (isCountingDown) {
                for (var i = firstRowIndex; i >= secondRowIndex; i--)
                    indices.unshift(i);
            } else {
                for (var i = firstRowIndex; i <= secondRowIndex; i++)
                    indices.unshift(i);
            }

            return indices;
        };

        ctor.prototype.copySelectedDocs = function () {
            this.showCopyDocDialog(false);
        };

        ctor.prototype.copySelectedDocIds = function () {
            this.showCopyDocDialog(true);
        };

        ctor.prototype.showCopyDocDialog = function (idsOnly) {
            var selectedDocs = this.getSelectedDocs();
            var copyDocumentsVm = new copyDocuments(selectedDocs);
            copyDocumentsVm.isCopyingDocs(idsOnly === false);
            app.showDialog(copyDocumentsVm);
        };

        ctor.prototype.getSelectedDocs = function (max) {
            if (!this.items || this.selectedIndices().length === 0) {
                return [];
            }

            var maxSelectedIndices = max ? this.selectedIndices.slice(0, max) : this.selectedIndices();
            return this.items.getCachedItemsAt(maxSelectedIndices);
        };

        ctor.prototype.deleteSelectedDocs = function () {
            var documents = this.getSelectedDocs();
            var deleteDocsVm = new deleteDocuments(documents);
            app.showDialog(deleteDocsVm);
        };
        ctor.idColumnWidth = 200;
        return ctor;
    })();

    
    return ctor;
});
//# sourceMappingURL=viewModel.js.map
