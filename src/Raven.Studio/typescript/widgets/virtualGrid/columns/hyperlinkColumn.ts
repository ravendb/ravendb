/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");

/**
 * Virtual grid column that renders hyperlinks.
 */
class hyperlinkColumn<T> extends textColumn<T> {

    private readonly hrefAccessor: (obj: T) => string;

    constructor(valueAccessor: ((obj: T) => any) | string, hrefAccessor: (obj: T) => string, header: string, width: string) {
        super(valueAccessor, header, width);

        this.hrefAccessor = hrefAccessor;
    }

    renderCell(item: T, isSelected: boolean): string {
        const hyperlinkValue = this.hrefAccessor(item);

        if (hyperlinkValue) {
            // decorate with link
            const preparedValue = this.prepareValue(item);

            return `<div class="cell text-cell ${preparedValue.typeCssClass}" style="width: ${this.width}"><a href="${hyperlinkValue}">${preparedValue.rawText}</a></div>`;
        } else {
            // fallback to plain text column
            return super.renderCell(item, isSelected);
        }
    }
}

export = hyperlinkColumn;