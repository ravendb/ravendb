/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

class storageReportItem {

    name: string;
    type: string;
    internalChildren: storageReportItem[];
    size?: number;
    length?: number;
    x?: number;
    y?: number;
    dx?: number;
    dy?: number;
    pageCount: number = null;
    parent?: storageReportItem;
    showType: boolean;
    w?: number; // used for storing text width
    numberOfEntries: number = null;
    lazyLoadChildren = false;
    customSizeProvider: (header: boolean) => string;
    isGrouped: boolean;
    
    recyclableJournal = false;

    link: string = null;
    dataSize: number = null;
    tempBuffersSize: number = null;

    constructor(name: string, type: string, showType: boolean, size: number, internalChildren: storageReportItem[] = null, isGrouped = false) {
        this.name = name;
        this.type = type;
        this.showType = showType;
        this.size = size;
        this.internalChildren = internalChildren;
        this.isGrouped = isGrouped;
    }

    formatSize(header: boolean) {
        return this.customSizeProvider ? this.customSizeProvider(header) : generalUtils.formatBytesToSize(this.size);
    }

    formatPercentage(parentSize: number) {
        return (this.size * 100 / parentSize).toFixed(2) + '%';
    }

    formatDataSize() {
        return this.dataSize == null ? "n/a" : generalUtils.formatBytesToSize(this.dataSize);
    }

    formatTempBuffersSize() {
        return this.tempBuffersSize == null ? "n/a" : generalUtils.formatBytesToSize(this.tempBuffersSize);
    }

    hasChildren(): boolean {
        return (this.internalChildren && this.internalChildren.length > 0) || (this.lazyLoadChildren === true);
    }
}

export = storageReportItem;
