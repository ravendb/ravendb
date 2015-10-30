interface timeSeriesTypeDto {
    Type: string;
    Fields: string[];
    KeysCount: number;
}

interface timeSeriesKeyDto {
    Type: timeSeriesTypeDto;
    Key: string;
    PointsCount: number;
}

interface timeSeriesKeySummaryDto {
    Type: timeSeriesTypeDto;
    Key: string;
    PointsCount: number;
    MinPoint: string;
    MaxPoint: string;
}

interface pointIdDto {
    Type: string;
    Key: string;
    At: string;
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
    Type: string;
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
