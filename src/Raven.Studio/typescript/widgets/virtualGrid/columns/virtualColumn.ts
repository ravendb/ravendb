/// <reference path="../../../../typings/tsd.d.ts"/>

interface virtualColumn {

    /**
     * The width string to use for the column. Example: "20px" or "10%".
     */
    width: string; // "20px" or "10%"

    /**
     * The text or HTML to display as the column header.
     */
    header: string;

    /**
     * The textual representation of header - used i.e. in columns selector
     */
    headerAsText: string;

    /**
     * Renders a cell for this column. Returns a string, either text or HTML, containing the content.
     */
    renderCell(item: Object, isSelected: boolean, isSorted: boolean): string;

    /**
     * Serialize column to json
     */
    toDto(): virtualColumnDto;

    /**
     * Returns true if column is sortable
     */
    sortable: boolean;
}

export = virtualColumn;
