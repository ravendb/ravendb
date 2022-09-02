/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import generalUtils = require("common/generalUtils");

class multiNodeTagsColumn<T extends object> implements virtualColumn {

    constructor(gridController: virtualGridController<T>,
                public valueAccessor: ((item: T) => string[]),
                public width: string,
                public opts: textColumnOpts<T> = {}) {
    }

    canHandle(actionId: string): boolean {
        return false;
    }

    get sortable() {
        return false;
    }

    header = `<div>Nodes</div>`;

    get headerAsText() {
        return this.header;
    }

    get headerTitle() {
        const titleToUse = this.opts && this.opts.headerTitle ? this.opts.headerTitle : this.header;
        return generalUtils.unescapeHtml(titleToUse);
    }

    renderCell(item: T): string {
        const extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
        
        return `<div class="cell text-cell ${extraCssClasses}" style="width: ${this.width}">${this.getValueHtml(this.getValue(item))}</div>`;
    }
    
    private getValue(item: T): string[] {
        return this.valueAccessor.bind(item)(item);
    }

    private getValueHtml(nodeTags: string[]): string {
        let result = "";
        
        if (nodeTags) {
            for (const tag of nodeTags) {
                const extraClass = `node-${tag}`;
                result += `<span class="node-label ${extraClass}">${tag}</span>`;
            }
        }

        return result;
    }

    toDto(): virtualColumnDto {
        return {
            type: "multiNodeTags",
            serializedValue: this.valueAccessor.toString(),
            width: this.width,
            header: null
        }
    }
}

export = multiNodeTagsColumn;
