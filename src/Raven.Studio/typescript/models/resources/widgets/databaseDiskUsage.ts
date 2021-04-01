/// <reference path="../../../../typings/tsd.d.ts" />

import generalUtils = require("common/generalUtils");

class databaseDiskUsage implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    size: string;
    tempBuffersSize: string;
    total: string;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.DatabaseDiskUsage) {
        this.nodeTag = nodeTag;
        this.database = data.Database;
        this.hideDatabaseName = false;
        this.size = generalUtils.formatBytesToSize(data.Size); 
        this.tempBuffersSize = generalUtils.formatBytesToSize(data.TempBuffersSize);
        this.total = generalUtils.formatBytesToSize(data.Size + data.TempBuffersSize);
    }
    
}

export = databaseDiskUsage;
