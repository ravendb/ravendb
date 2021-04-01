
class trafficWatchItem implements databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;

    requestsPerSecond: number;
    writesPerSecond: number;
    dataWritesPerSecond: number;
    
    hideDatabaseName: boolean;
    even: boolean = false;
    
    constructor(nodeTag: string, data: Raven.Server.Dashboard.TrafficWatchItem) {
        this.nodeTag = nodeTag;
        this.database = data.Database;
        this.hideDatabaseName = false;
        
        this.requestsPerSecond = data.RequestsPerSecond;
        this.writesPerSecond = data.DocumentWritesPerSecond + data.AttachmentWritesPerSecond + data.CounterWritesPerSecond + data.TimeSeriesWritesPerSecond;
        this.dataWritesPerSecond = data.DocumentsWriteBytesPerSecond + data.AttachmentsWriteBytesPerSecond + data.CountersWriteBytesPerSecond + data.TimeSeriesWriteBytesPerSecond;
    }
    
}

export = trafficWatchItem;
