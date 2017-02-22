/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

/**
 * Checked cell template used for toggling selection of a row in the virtual grid.
 */
class checkedColumn implements virtualColumn {
    readonly header = `<div class="checkbox checkbox-table-selector"><input class="checked-column-header styled" type="checkbox" /><label></label></div>`;
    readonly width = "38px";

    renderCell(item: Object, isSelected: boolean): string {
        if (isSelected) {
            return `<div class="cell checkbox checkbox-table-selector"><input class="checked-cell-input styled" type="checkbox" checked /><label></label></div>`;
        }

        return `<div class="cell checkbox checkbox-table-selector"><input class="checked-cell-input styled" type="checkbox" /><label></label></div>`;
    }
}

export = checkedColumn;