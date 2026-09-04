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

    // shard which owns the bucket according to the sharding configuration - set only when the bucket
    // temporarily resides on more than one shard (resharding in progress)
    ownerShard: number = null;

    constructor(name: string, size: number, numberOfBuckets: number, documentsCount: number, shards: number[], internalChildren: bucketReportItem[] = null) {
        this.name = name;
        this.size = size;
        this.numberOfBuckets = numberOfBuckets;
        this.documentsCount = documentsCount;
        this.shards = shards;
        this.internalChildren = internalChildren;
    }

    isShardPendingRemoval(shard: number): boolean {
        return this.ownerShard != null && this.shards.length > 1 && shard !== this.ownerShard;
    }

    pendingRemovalTooltip(): string {
        return "This bucket is being moved to shard #" + this.ownerShard + ". The copy on this shard will be removed once resharding completes.";
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
