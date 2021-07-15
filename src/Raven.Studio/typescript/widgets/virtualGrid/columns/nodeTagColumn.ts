/// <reference path="../../../../typings/tsd.d.ts"/>

import virtualGridController = require("widgets/virtualGrid/virtualGridController");
import hyperlinkColumn = require("widgets/virtualGrid/columns/hyperlinkColumn");

class nodeTagColumn<T extends { nodeTag: string, database: string, noData: boolean, isCommonItem: boolean }> extends hyperlinkColumn<T> {

    hrefDestination: string;
    
    constructor(gridController: virtualGridController<any>, hrefProvider: (item: T) => { url: string; openInNewTab: boolean }, hrefDestination: string) {
        super(gridController, item => this.valueProvider(item), item => hrefProvider(item).url, "Node", "70px", {
            useRawValue: () => true,
            openInNewTab: item => hrefProvider(item).openInNewTab
        });
        
        this.hrefDestination = hrefDestination;
    }
    
    private valueProvider(item: T) {
        const nodeTag = item.nodeTag;
        const extraClass = item.noData ? "no-data" : `node-${nodeTag}`;
        
        if (item.isCommonItem) {
            return "";
        } else {
            return `<span class="node-label ${extraClass}" title="Go to ${this.hrefDestination} view">${nodeTag}</span>`;
        }
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
