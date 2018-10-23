/// <reference path="../../../../typings/tsd.d.ts"/>
import textColumn = require("widgets/virtualGrid/columns/textColumn");
import virtualRow = require("widgets/virtualGrid/virtualRow");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");

type hypertextColumnOpts<T> = {
    extraClass?: (item: T) => string;
    useRawValue?: (item: T) => boolean;
    handler?: (item: T, event: JQueryEventObject) => void;
}

/**
 * Virtual grid column that renders hyperlinks.
 */
class hyperlinkColumn<T> extends textColumn<T> {

    private readonly hrefAccessor: (obj: T) => string;
    private readonly customHandler: (obj: T, event: JQueryEventObject) => void;

    linkActionUniqueId = _.uniqueId("link-action-");

    constructor(gridController: virtualGridController<T>, valueAccessor: ((obj: T) => any) | string, hrefAccessor: (obj: T) => string, header: string, width: string, opts: hypertextColumnOpts<T> = {}) {
        super(gridController, valueAccessor, header, width, opts);

        this.hrefAccessor = hrefAccessor;
        this.customHandler = opts.handler;
    }

    canHandle(actionId: string) {
        return this.linkActionUniqueId === actionId;
    }

    handle(row: virtualRow, event: JQueryEventObject) {
        this.customHandler(row.data as T, event);
    }

    renderCell(item: T, isSelected: boolean): string {
        const hyperlinkValue = this.hrefAccessor(item);

        if (hyperlinkValue) {
            // decorate with link
            const preparedValue = this.prepareValue(item);
            const extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
            const customAction = this.customHandler ? `data-link-action="${this.linkActionUniqueId}"` : "";

            return `<div class="cell text-cell ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}"><a href="${hyperlinkValue}" ${customAction}>${preparedValue.rawText}</a></div>`;
        } else {
            // fallback to plain text column
            return super.renderCell(item, isSelected);
        }
    }

    toDto(): virtualColumnDto {
        return {
            type: "hyperlink",
            header: this.header,
            width: this.width,
            serializedValue: this.valueAccessor.toString()
        };
    }
}

export = hyperlinkColumn;
