import virtualColumn = require("widgets/virtualGrid/virtualColumn");

/**
 * Checked cell template used for toggling selection of a row in the virtual grid.
 */
class checkedColumn implements virtualColumn {
    public readonly cellClass = "checked-cell";
    public readonly dataMemberName = "__virtual-grid-isChecked";
    public readonly width = "32px";
    public readonly display = `<input class="checked-column-header" type="checkbox" />`;

    public static readonly columnWidth = 32;

    constructor() {
    }

    renderCell(item: Object, isSelected: boolean): string {
        if (isSelected) {
            return `<input class="checked-cell-input" type="checkbox" checked />`;
        }

        return `<input class="checked-cell-input" type="checkbox" />`;
    }
}

export = checkedColumn;