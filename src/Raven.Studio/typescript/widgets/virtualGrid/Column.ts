import CellTemplate = require("widgets/virtualGrid/CellTemplate");

interface Column {
    /**
     * The name of the data field for the column.
     */
    dataMemberName: string;

    /**
     * The text or HTML to display as the column header.
     */
    display: string;

    /**
     * The template to use for the cells in this column. If null, the column will default to TextCellTemplate.
     */
    template: CellTemplate | null;

    /**
     * The width string to use for the column. Example: "20px" or "10%".
     */
    width: string; // "20px" or "10%"
}

export = Column;