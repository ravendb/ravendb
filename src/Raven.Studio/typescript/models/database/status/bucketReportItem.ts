/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

class bucketReportItem {
    name: string;
    internalChildren: bucketReportItem[];
    size?: number;
    length?: number;
    x?: number;
    y?: number;
    dx?: number;
    dy?: number;
    
    fromRange: number;
    toRange: number;
    
    documentsCount: number = null;
    parent?: bucketReportItem;
    w?: number; // used for storing text width
    lazyLoadChildren = false;
    
    shards: number[] = [];
    
    constructor(name: string, size: number, documentsCount: number, shards: number[], internalChildren: bucketReportItem[] = null) {
        this.name = name;
        this.size = size;
        this.documentsCount = documentsCount;
        this.shards = shards;
        this.internalChildren = internalChildren;
    }

    formatSize() {
        return generalUtils.formatBytesToSize(this.size);
    }

    formatPercentage(parentSize: number) {
        return (this.size * 100 / parentSize).toFixed(2) + '%';
    }

    hasChildren(): boolean {
        return (this.internalChildren && this.internalChildren.length > 0) || (this.lazyLoadChildren === true);
    }
}

export = bucketReportItem;
