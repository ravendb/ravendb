/// <reference path="../../../../typings/tsd.d.ts" />

class databaseDiskUsage implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    size: number;
    tempBuffersSize: number;
    total: number;
    noData: boolean;
    
    hideDatabaseName: boolean;
    even = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.DatabaseDiskUsage) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.size = data.Size;
            this.tempBuffersSize = data.TempBuffersSize;
            this.total = data.Size + data.TempBuffersSize;    
        } else {
            this.noData = true;
        }
    }

    static noData(nodeTag: string, database: string): databaseDiskUsage {
        const item = new databaseDiskUsage(nodeTag, null);
        item.database = database;
        return item;
    }
    
}

export = databaseDiskUsage;
