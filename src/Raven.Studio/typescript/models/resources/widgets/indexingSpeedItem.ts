/// <reference path="../../../../typings/tsd.d.ts" />


class indexingSpeedItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    indexedPerSecond: number;
    mappedPerSecond: number;
    reducedPerSecond: number;
    noData: boolean;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.IndexingSpeedItem) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.indexedPerSecond = indexingSpeedItem.roundNumber(data.IndexedPerSecond);
            this.mappedPerSecond = indexingSpeedItem.roundNumber(data.MappedPerSecond);
            this.reducedPerSecond = indexingSpeedItem.roundNumber(data.ReducedPerSecond);
        } else {
            this.noData = true;
        }
    }
    
    private static roundNumber(input: number) {
        if (input > 1) {
            return Math.round(input);
        }
        return input;
    }
    
    static noData(nodeTag: string, database: string): indexingSpeedItem {
        const item = new indexingSpeedItem(nodeTag, null);
        item.database = database;
        return item;
    }
}

export = indexingSpeedItem;
