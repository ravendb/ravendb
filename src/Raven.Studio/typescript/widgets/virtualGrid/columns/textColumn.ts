/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import utils = require("widgets/virtualGrid/virtualGridUtils");

type preparedValue = {
    rawText: string;
    typeCssClass: string;
    title: string;
}

class textColumn<T> implements virtualColumn {
    constructor(
        public valueAccessor: (item: T) => any,
        public header: string, 
        public width: string) {
    }

    renderCell(item: T, isSelected: boolean): string {
        const preparedValue = this.prepareValue(item);
        return `<div class="cell text-cell ${preparedValue.typeCssClass}" title="${preparedValue.title}" style="width: ${this.width}">${preparedValue.rawText}</div>`;
    }

    protected prepareValue(item: T): preparedValue {
        const cellValue = this.valueAccessor(item);

        if (_.isString(cellValue)) {
            const rawText = utils.escape(cellValue);
            return {
                rawText: rawText,
                title: rawText,
                typeCssClass: "token-string"
            };
        }

        if (_.isNumber(cellValue)) {
            const value = cellValue.toLocaleString();
            return {
                rawText: value,
                title: value,
                typeCssClass: "token-number"
            };
        }

        if (_.isBoolean(cellValue)) {
            const value = !!cellValue;
            return {
                rawText: value ? 'true' : 'false',
                title: value ? 'true' : 'false',
                typeCssClass: "token-boolean"
            }
        }

        if (_.isNull(cellValue)) {
            return {
                rawText: "null",
                title: "null",
                typeCssClass: "token-null"
            }
        }

        if (_.isUndefined(cellValue)) {
            return {
                rawText: "",
                title: "",
                typeCssClass: "token-undefined"
            }
        }

        if (_.isArray(cellValue)) {
            const value = utils.escape(JSON.stringify(cellValue, null, 2));

            return {
                rawText: "[ ... ]",
                title: value,
                typeCssClass: "token-array"
            }
        }

        if (_.isObject(cellValue)) {
            const value = utils.escape(JSON.stringify(cellValue, null, 2));

            return {
                rawText: "{ ... }",
                title: value,
                typeCssClass: "token-object"
            }
        }

        if (cellValue != null) {
            const value = utils.escape(cellValue.toString());
            return {
                rawText: value,
                title: value,
                typeCssClass: ""
            };
        }

        throw new Error("Unhandled value: " + cellValue);
    }

}

export = textColumn;