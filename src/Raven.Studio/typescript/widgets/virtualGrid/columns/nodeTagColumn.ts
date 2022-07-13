/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");

class nodeTagColumn<T extends { nodeTag: string, noData: boolean }> extends hyperlinkColumn<T> {

    hrefProvider: (item: T) => { url: string; openInNewTab: boolean; targetDescription?: string };
    
    constructor(gridController: virtualGridController<any>, 
                hrefProvider: (item: T) => { url: string; openInNewTab: boolean; noData: boolean; targetDescription?: string }) {
        super(gridController, item => this.valueProvider(item), item => hrefProvider(item).url, "Node", "70px", {
            useRawValue: () => true,
            openInNewTab: item => hrefProvider(item).openInNewTab
        });
        
        this.hrefProvider = hrefProvider;
    }
    
    private valueProvider(item: T) {
        const nodeTag = item.nodeTag;
        
        if (!nodeTag) {
            return "";
        }

        const description = this.hrefProvider(item).targetDescription;
            const titleText = description ? `Go to ${description} on node ${nodeTag}` : "";
        
        const nodeDataClass = item.noData ? "no-data" : `node-${nodeTag}`

        return `<span class="node-label ${nodeDataClass}" title="${titleText}">${nodeTag}</span>`;
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
