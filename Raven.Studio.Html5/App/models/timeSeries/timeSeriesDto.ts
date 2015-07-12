interface timeSeriesKeyDto {
    Prefix: string;
    Key: string;
    Count: number;
}

interface timeSeriesPointDto {
    Prefix: string;
    Key: string;
    At: number;
    Values: number[];
}

interface statusDebugChangesTimeSeriesDto {
    WatchedKeyChanges: Array<string>;
    WatchedBulkOperationsChanges: Array<string>;
}

interface timeSeriesKeyChangeNotification {
    Prefix: string;
    Key: string;
    Action: string;
    At: number;
    Values: number[];
    Start: number;
    End: number;
}

interface timeSeriesBulkOperationNotificationDto {
    OperationId: string;
    BatchType: string;
    Message: string;
}