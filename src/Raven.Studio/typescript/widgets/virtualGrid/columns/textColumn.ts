/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import utils = require("widgets/virtualGrid/virtualGridUtils");

type textColumnOpts<T> = {
    extraClass?: (item: T) => string;
    useRawValue?: (item: T) => boolean;
    title?: (item:T) => string;
}

type preparedValue = {
    rawText: string;
    typeCssClass: string;
}

class textColumn<T> implements virtualColumn {
    constructor(
        protected gridController: virtualGridController<T>,
        public valueAccessor: ((item: T) => any) | string,
        public header: string, 
        public width: string,
        public opts: textColumnOpts<T> = {}) {
    }
    
    get headerTitle() {
        return this.header;
    }

    get headerAsText() {
        return this.header;
    }

    getCellValue(item: T) {
        return _.isFunction(this.valueAccessor)
            ? this.valueAccessor.bind(item)(item) // item is available as this, as well as first argument
            : (item as any)[this.valueAccessor as string];
    }

    renderCell(item: T, isSelected: boolean): string {
        const extraHtml = this.opts.title ? ` title="${utils.escape(this.opts.title(item))}" ` : '';
        const extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
        try {
            const preparedValue = this.prepareValue(item);
            return `<div  ${extraHtml} class="cell text-cell ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}">${preparedValue.rawText}</div>`;
        } catch (error) {
            //TODO: work on L&F of errors!
            return `<div class="cell text-cell eval-error ${extraCssClasses}" style="width: ${this.width}">Error!</div>`;
        }
        
    }

    protected prepareValue(item: T): preparedValue {
        const cellValue = this.getCellValue(item);

        if (_.isString(cellValue)) {
            const rawText = this.opts.useRawValue && this.opts.useRawValue(item) ? cellValue : utils.escape(cellValue);
            return {
                rawText: rawText,
                typeCssClass: "token string"
            };
        }

        if (_.isNumber(cellValue)) {
            const value = cellValue.toLocaleString();
            return {
                rawText: value,
                typeCssClass: "token number"
            };
        }

        if (_.isBoolean(cellValue)) {
            const value = !!cellValue;
            return {
                rawText: value ? 'true' : 'false',
                typeCssClass: "token boolean"
            }
        }

        if (_.isNull(cellValue)) {
            return {
                rawText: "null",
                typeCssClass: "token null"
            }
        }

        if (_.isUndefined(cellValue)) {
            return {
                rawText: "",
                typeCssClass: "token undefined"
            }
        }

        if (_.isArray(cellValue)) {
            const value = utils.escape(JSON.stringify(cellValue, null, 2));

            return {
                rawText: "[ ... ]",
                typeCssClass: "token array"
            }
        }

        if (_.isObject(cellValue)) {
            const value = utils.escape(JSON.stringify(cellValue, null, 2));

            return {
                rawText: "{ ... }",
                typeCssClass: "token object"
            }
        }

        if (cellValue != null) {
            const value = utils.escape(cellValue.toString());
            return {
                rawText: value,
                typeCssClass: ""
            };
        }

        throw new Error("Unhandled value: " + cellValue);
    }
    
    toDto(): virtualColumnDto {
        return {
            type: "text",
            width: this.width,
            header: this.header,
            serializedValue: this.valueAccessor.toString()
        }
    }

}

export = textColumn;
