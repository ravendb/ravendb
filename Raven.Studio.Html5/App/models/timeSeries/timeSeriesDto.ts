interface timeSeriesKeyDto {
    Prefix: string;
    ValueLength: number;
    Key: string;
    PointsCount: number;
}

interface pointDto {
    At: string;
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
    At: string;
    Values: number[];
    Start: number;
    End: number;
}

interface timeSeriesBulkOperationNotificationDto {
    OperationId: string;
    BatchType: string;
    Message: string;
}