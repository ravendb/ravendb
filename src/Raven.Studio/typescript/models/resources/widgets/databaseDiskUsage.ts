/// <reference path="../../../../typings/tsd.d.ts" />

import generalUtils = require("common/generalUtils");

class databaseDiskUsage implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    size: string;
    tempBuffersSize: string;
    total: string;
    noData: boolean;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.DatabaseDiskUsage) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.size = generalUtils.formatBytesToSize(data.Size);
            this.tempBuffersSize = generalUtils.formatBytesToSize(data.TempBuffersSize);
            this.total = generalUtils.formatBytesToSize(data.Size + data.TempBuffersSize);    
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
