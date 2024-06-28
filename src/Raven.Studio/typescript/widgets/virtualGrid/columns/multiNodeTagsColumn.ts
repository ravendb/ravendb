/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import generalUtils = require("common/generalUtils");

class multiNodeTagsColumn<T extends object> implements virtualColumn {
    private readonly nodeHrefAccessor: (item: T, nodeTag: string) => string;
    private readonly nodeLinkTitleAccessor: (item: T, nodeTag: string) => string;

    constructor(gridController: virtualGridController<T>,
        public valueAccessor: ((item: T) => string[]),
        public width: string,
        public opts: multiNodeTagsColumnOpts<T> = {}
    ) {
        this.nodeHrefAccessor = opts.nodeHrefAccessor;
        this.nodeLinkTitleAccessor = opts.nodeLinkTitleAccessor;
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
        
        return `<div class="cell text-cell ${extraCssClasses}" style="width: ${this.width}; display: flex">${this.getValueHtml(item)}</div>`;
    }
    
    private getValue(item: T): string[] {
        return this.valueAccessor.bind(item)(item);
    }

    private getValueHtml(item: T): string {
        let result = "";
        const nodeTags = this.getValue(item);

        if (nodeTags) {
            for (const tag of nodeTags) {
                const extraClass = `node-${tag}`;

                let nodeHtml = `<span class="node-label ${extraClass}">${tag}</span>`;
                
                if (this.nodeHrefAccessor) {
                    const href = this.nodeHrefAccessor(item, tag);
                    const title = this.nodeLinkTitleAccessor ? this.nodeLinkTitleAccessor(item, tag) : tag;

                    nodeHtml = `<a href="${href}" title="${title}">${nodeHtml}</a>`;
                }

                result += nodeHtml;
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
