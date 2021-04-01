/// <reference path="../../../../typings/tsd.d.ts" />


class indexingSpeedItem {
    database: string;
    nodeTag: string;
    indexedPerSecond: number;
    mappedPerSecond: number;
    reducedPerSecond: number;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.IndexingSpeedItem) {
        this.nodeTag = nodeTag;
        this.database = data.Database;
        this.hideDatabaseName = false;
        this.indexedPerSecond = indexingSpeedItem.roundNumber(data.IndexedPerSecond);
        this.mappedPerSecond = indexingSpeedItem.roundNumber(data.MappedPerSecond);
        this.reducedPerSecond = indexingSpeedItem.roundNumber(data.ReducedPerSecond);
    }
    
    private static roundNumber(input: number) {
        if (input > 1) {
            return Math.round(input);
        }
        return input;
    }
}

export = indexingSpeedItem;
