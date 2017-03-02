/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

/**
 * Checked cell template used for toggling selection of a row in the virtual grid.
 */
class checkedColumn implements virtualColumn {
    private static readonly headerWithCheckbox = `<div class="checkbox checkbox-table-selector"><input class="checked-column-header styled" type="checkbox" /><label></label></div>`;
    private static readonly headerWithoutCheckbox = `<div class="checkbox checkbox-table-selector">&nbsp;</div>`;

    readonly width = "38px";

    private readonly withSelectAll: boolean;

    constructor(withSelectAll: boolean) {
        this.withSelectAll = withSelectAll;
    }

    get header() {
        return this.withSelectAll ? checkedColumn.headerWithCheckbox : checkedColumn.headerWithoutCheckbox;
    }

    renderCell(item: Object, isSelected: boolean): string {
        if (isSelected) {
            return `<div class="cell checkbox checkbox-table-selector"><input class="checked-cell-input styled" type="checkbox" checked /><label></label></div>`;
        }

        return `<div class="cell checkbox checkbox-table-selector"><input class="checked-cell-input styled" type="checkbox" /><label></label></div>`;
    }
}

export = checkedColumn;