/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");

/**
 * Virtual grid column that renders hyperlinks.
 */
class hyperlinkColumn implements virtualColumn {

    /**
     * Creates a new hyperlink column.
     * @param dataMemberName The name of the property containing the text to display in the hyperlink.
     * @param hrefMemberName The name of the property containing the link.
     * @param display The column header text.
     * @param width The width of the column, e.g. "20px" or "100%"
     */
    constructor(
        public dataMemberName: string,
        public hrefMemberName: string,
        public header: string,
        public width: string) {
    }

    renderCell(item: Object, isSelected: boolean): string {
        const cellValue: Object | null = (item as any)[this.dataMemberName];
        const hyperlinkValue: Object | null = (item as any)[this.hrefMemberName];

        //TODO: get title
        return `<div class="cell hyperlink-cell"><a href="${hyperlinkValue ? hyperlinkValue.toString() : "javascript:void(0)"}">${cellValue ? cellValue.toString() : ""}</a></div>`;
    }
}

export = hyperlinkColumn;