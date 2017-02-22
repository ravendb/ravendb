/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import utils = require("widgets/virtualGrid/virtualGridUtils");

/**
 * Virtual grid column that renders text in its cells.
 */
class textColumn implements virtualColumn {
    readonly cellClass = "text-cell";

    constructor(
        public dataMemberName: string,
        public header: string, 
        public width: string) {
    }

    renderCell(item: Object, isSelected: boolean): string {
        const rawValue = this.prepareCellValue(item, isSelected);
        return `<div class="cell text-cell" style="width: ${this.width}">${rawValue}</div>`;
    }

    private prepareCellValue(item: Object, isSelected: boolean): string {
        const cellValue = (item as any)[this.dataMemberName];
        if (_.isString(cellValue)) {
            return utils.escape(cellValue);
        }

        if (_.isArray(cellValue)) {
            return "[ ... ]";
        }

        if (_.isNumber(cellValue)) {
            return cellValue.toLocaleString();
        }

        if (_.isObject(cellValue)) {
            return "{ ... }";
        }

        if (cellValue != null) {
            return utils.escape(cellValue.toString());
        }

        return "";
    }


}

export = textColumn;