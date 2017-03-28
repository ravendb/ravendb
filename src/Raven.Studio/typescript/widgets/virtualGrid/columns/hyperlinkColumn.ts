/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");

type hypertextColumnOpts<T> = {
    extraClass?: (item: T) => string;
}

/**
 * Virtual grid column that renders hyperlinks.
 */
class hyperlinkColumn<T> extends textColumn<T> {

    private readonly hrefAccessor: (obj: T) => string;

    constructor(valueAccessor: ((obj: T) => any) | string, hrefAccessor: (obj: T) => string, header: string, width: string, opts: hypertextColumnOpts<T> = {}) {
        super(valueAccessor, header, width, opts);

        this.hrefAccessor = hrefAccessor;
    }

    renderCell(item: T, isSelected: boolean): string {
        const hyperlinkValue = this.hrefAccessor(item);

        if (hyperlinkValue) {
            // decorate with link
            const preparedValue = this.prepareValue(item);
            const extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';

            return `<div class="cell text-cell ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}"><a href="${hyperlinkValue}">${preparedValue.rawText}</a></div>`;
        } else {
            // fallback to plain text column
            return super.renderCell(item, isSelected);
        }
    }
}

export = hyperlinkColumn;