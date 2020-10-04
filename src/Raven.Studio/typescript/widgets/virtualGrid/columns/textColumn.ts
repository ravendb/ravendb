/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import generalUtils = require("common/generalUtils");

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
        return generalUtils.unescapeHtml(this.header);
    }

    get headerAsText() {
        return this.header;
    }
    
    get sortable(): boolean {
        return this.opts && !!this.opts.sortable;
    }

    canHandle(actionId: string) {
        return false;
    }

    sortProvider(mode: sortMode): (array: Array<any>) => Array<any> {
        if (this.opts && this.opts.sortable) {
            
            const multiplier = mode === "asc" ? 1 : -1;
            switch (this.opts.sortable) {
                case "string":
                case "number":
                    return this.opts.customComparator ? 
                        (input: Array<any>) => input.sort((a, b) => multiplier * this.opts.customComparator(this.getCellValue(a), this.getCellValue(b)))
                        : (input: Array<any>) => _.orderBy(input, x => this.getCellValue(x), mode);
                default:
                    const provider = this.opts.sortable as valueProvider<T>;
                    
                    return this.opts.customComparator ?
                        (input: Array<any>) => input.sort((a, b) => multiplier * this.opts.customComparator(provider(a), provider(b)))
                        : (input: Array<any>) => _.orderBy(input, x => provider(x), mode);
                    
            }
        }
        return null;
    }

    get defaultSortOrder(): sortMode {
        if (this.opts && this.opts.defaultSortOrder) {
            return this.opts.defaultSortOrder;
        }
        
        return "asc";
    }
    
    getCellValue(item: T) {
        return _.isFunction(this.valueAccessor)
            ? this.valueAccessor.bind(item)(item) // item is available as this, as well as first argument
            : (item as any)[this.valueAccessor as string];
    }

    renderCell(item: T, isSelected: boolean, isSorted: boolean): string {
        const titleHtml = this.opts.title ? ` title="${generalUtils.escapeHtml(this.opts.title(item))}" ` : '';
        let extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
        
        if (isSorted) {
            extraCssClasses += ' sorted';
        }
        
        try {
            const preparedValue = this.prepareValue(item);
            return `<div  ${titleHtml} class="cell text-cell ${preparedValue.typeCssClass} ${extraCssClasses}" style="width: ${this.width}">${preparedValue.rawText}</div>`;
        } catch (error) {
            //TODO: work on L&F of errors!
            return `<div class="cell text-cell eval-error ${extraCssClasses}" style="width: ${this.width}">Error!</div>`;
        }
    }

    protected prepareValue(item: T): preparedValue {
        const cellValue = this.getCellValue(item);

        if (_.isString(cellValue)) {
            let cssClass = "token";
            if (this.opts.extraClass && !this.opts.extraClass(item).includes("no-color")) {
                cssClass += " string";
            }
            
            const rawText = this.opts.useRawValue && this.opts.useRawValue(item) ? cellValue : generalUtils.escapeHtml(cellValue);
            
            return {
                rawText: rawText,
                typeCssClass: cssClass
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
            const innerHtml = cellValue.length ? "&hellip;" : "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";

            return {
                rawText: `<span class="array-item">[${innerHtml}]</span> <sup>${cellValue.length}</sup>`,
                typeCssClass: "token array"
            }
        }

        if (_.isObject(cellValue)) {
            const propertiesCount = Object.keys(cellValue).length;
            const innerHtml = propertiesCount ? "&hellip;" : "&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;";

            return {
                rawText: `<span class="object-item">{${innerHtml}}</span> <sup>${propertiesCount}</sup>`,
                typeCssClass: "token object"
            }
        }

        if (cellValue != null) {
            const value = generalUtils.escapeHtml(cellValue.toString());
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
