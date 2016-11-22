/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

class storageReportItem {

    name: string;
    type: string;
    internalChildren: storageReportItem[];
    size?: number;
    x?: number;
    y?: number;
    dx?: number;
    dy?: number;
    pageCount: number = null;
    parent?: storageReportItem;
    showType: boolean;
    w?: number; // used for storing text width
    numberOfEntries: number = null;


    constructor(name: string, type: string, showType: boolean, size: number, internalChildren: storageReportItem[] = null) {
        this.name = name;
        this.type = type;
        this.showType = showType;
        this.size = size;
        this.internalChildren = internalChildren;
    }

    formatSize() {
        return generalUtils.formatBytesToSize(this.size);
    }

    formatPercentage(parentSize: number) {
        return (this.size * 100 / parentSize).toFixed(2) + '%';
    }

    hasChildren() {
        return this.internalChildren && this.internalChildren.length;
    }

}

export = storageReportItem;