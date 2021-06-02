
class trafficWatchItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;

    requestsPerSecond: number;
    writesPerSecond: number;
    dataWritesPerSecond: number;
    noData: boolean;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.TrafficWatchItem) {
        this.nodeTag = nodeTag;
        this.hideDatabaseName = false;
        
        if (data) {
            this.noData = false;
            this.database = data.Database;
            this.requestsPerSecond = data.RequestsPerSecond;
            this.writesPerSecond = data.DocumentWritesPerSecond + data.AttachmentWritesPerSecond + data.CounterWritesPerSecond + data.TimeSeriesWritesPerSecond;
            this.dataWritesPerSecond = data.DocumentsWriteBytesPerSecond + data.AttachmentsWriteBytesPerSecond + data.CountersWriteBytesPerSecond + data.TimeSeriesWriteBytesPerSecond;
        } else {
            this.noData = true;
        }
    }

    static noData(nodeTag: string, database: string): trafficWatchItem {
        const item = new trafficWatchItem(nodeTag, null);
        item.database = database;
        return item;
    }
    
}

export = trafficWatchItem;
