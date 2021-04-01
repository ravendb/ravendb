/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("./hyperlinkColumn");

class nodeTagColumn<T extends { nodeTag: string, database: string }> extends hyperlinkColumn<T> {
    
    constructor(gridController: virtualGridController<any>, hrefProvider: (item: T) => { url: string; openInNewTab: boolean }) {
        super(gridController, item => this.valueProvider(item), item => hrefProvider(item).url, "Node", "70px", {
            useRawValue: () => true,
            openInNewTab: item => hrefProvider(item).openInNewTab
        });
    }
    
    private valueProvider(item: T) {
        const nodeTag = item.nodeTag;
        return `<span class="node-label node-${nodeTag.toLocaleLowerCase()}">${nodeTag}</span>`;
    }

    toDto(): virtualColumnDto {
        return {
            type: "nodeTag",
            serializedValue: null,
            width: this.width,
            header: null
        }
    }
}

export = nodeTagColumn;
