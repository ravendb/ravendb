/// <reference path="../../../../typings/tsd.d.ts"/>

import generalUtils = require("common/generalUtils");

class bucketReportItem {
    name: string;
    internalChildren: bucketReportItem[];
    size?: number;
    x?: number;
    y?: number;
    dx?: number;
    dy?: number;
    
    fromRange: number;
    toRange: number;
    
    documentsCount: number = null;
    numberOfBuckets: number;
    parent?: bucketReportItem;
    w?: number; // used for storing text width
    lazyLoadChildren = false;
    
    shards: number[] = [];
    
    constructor(name: string, size: number, numberOfBuckets: number, documentsCount: number, shards: number[], internalChildren: bucketReportItem[] = null) {
        this.name = name;
        this.size = size;
        this.numberOfBuckets = numberOfBuckets;
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

    private static columnValue(item: bucketReportItem, column: bucketReportSortColumn): number {
        switch (column) {
            case "range":
                return item.fromRange;
            case "buckets":
                return item.numberOfBuckets;
            case "documents":
                return item.documentsCount;
            case "size":
                return item.size;
        }
    }

    private static defaultDirection(column: bucketReportSortColumn): bucketReportSortDirection {
        return column === "range" ? "asc" : "desc";
    }

    static nextSort(current: bucketReportSort, column: bucketReportSortColumn): bucketReportSort {
        const defaultDirection = bucketReportItem.defaultDirection(column);

        if (!current || current.column !== column) {
            return { column, direction: defaultDirection };
        }

        if (current.direction === defaultDirection) {
            return { column, direction: defaultDirection === "asc" ? "desc" : "asc" };
        }

        return null;
    }

    static sortComparator(column: bucketReportSortColumn, direction: bucketReportSortDirection) {
        const multiplier = direction === "asc" ? 1 : -1;

        return (a: bucketReportItem, b: bucketReportItem) => {
            const difference = bucketReportItem.columnValue(a, column) - bucketReportItem.columnValue(b, column);

            return difference ? difference * multiplier : a.fromRange - b.fromRange;
        };
    }
}

export = bucketReportItem;
