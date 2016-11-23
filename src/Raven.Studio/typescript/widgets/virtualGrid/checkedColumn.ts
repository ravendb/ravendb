import virtualColumn = require("widgets/virtualGrid/virtualColumn");

/**
 * Checked cell template used for toggling selection of a row in the virtual grid.
 */
class checkedColumn implements virtualColumn {
    readonly cellClass = "checked-cell";
    readonly dataMemberName = "__virtual-grid-isChecked";
    readonly width = "32px";
    readonly display = `<input class="checked-column-header" type="checkbox" />`;

    static readonly columnWidth = 32;
    
    renderCell(item: Object, isSelected: boolean): string {
        if (isSelected) {
            return `<input class="checked-cell-input" type="checkbox" checked />`;
        }

        return `<input class="checked-cell-input" type="checkbox" />`;
    }
}

export = checkedColumn;