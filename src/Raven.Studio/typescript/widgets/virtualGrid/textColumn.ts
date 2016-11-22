import virtualColumn = require("widgets/virtualGrid/virtualColumn");

/**
 * Virtual grid column that renders text in its cells.
 */
class textColumn implements virtualColumn {
    public readonly cellClass = "text-cell";

    constructor(
        public dataMemberName: string,
        public display: string,
        public width: string) {
    }

    renderCell(item: Object, isSelected: boolean): string {
        const cellValue = (item as any)[this.dataMemberName];
        if (cellValue != null) {
            return cellValue.toString();
        }

        return "";
    }
}

export = textColumn;