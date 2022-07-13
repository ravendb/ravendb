/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import virtualColumn = require("widgets/virtualGrid/columns/virtualColumn");
import taskItem = require("models/resources/widgets/taskItem");
import generalUtils = require("common/generalUtils");

class multiNodeTagsColumn implements virtualColumn {

    constructor(gridController: virtualGridController<any>,
                public width: string,
                public opts: textColumnOpts<taskItem> = {}) {
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

    renderCell(item: taskItem): string {
        let extraCssClasses = this.opts.extraClass ? this.opts.extraClass(item) : '';
        
        return `<div class="cell text-cell ${extraCssClasses}" style="width: ${this.width}">${this.valueProvider(item)}</div>`;
    }
    
    private valueProvider(item: taskItem) {
        let result = "";
        
        if (item.nodeTags()) {
            for (let i = 0; i < item.nodeTags().length; i++) {
                const nodeTag = item.nodeTags()[i];
                
                const extraClass = `node-${nodeTag}`;
                result += `<span class="node-label ${extraClass}">${nodeTag}</span>`;
            }
        }

        return result;
    }

    toDto(): virtualColumnDto {
        return {
            type: "multiNodeTags",
            serializedValue: null,
            width: this.width,
            header: null
        }
    }
}

export = multiNodeTagsColumn;
