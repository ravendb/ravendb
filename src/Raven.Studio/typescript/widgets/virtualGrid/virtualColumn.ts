import cellTemplate = require("widgets/virtualGrid/cellTemplate");

interface column {
    /**
     * The name of the data field for the column.
     */
    dataMemberName: string;

    /**
     * The text or HTML to display as the column header.
     */
    display: string;

    /**
     * The width string to use for the column. Example: "20px" or "10%".
     */
    width: string; // "20px" or "10%"

    /**
     * The CSS class to apply to the container of the cell.
     */
    cellClass: string;

    /**
     * Renders a cell for this column. Returns a string, either text or HTML, containing the content.
     */
    renderCell(item: Object, isSelected: boolean): string;
}

export = column;