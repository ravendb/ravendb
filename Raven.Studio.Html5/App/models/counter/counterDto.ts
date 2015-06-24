interface counterStorageDto {
    Name: string;
    Path?: string;
}

interface counterDto {
    CurrentValue: number;
    Group: string;
    CounterName: string;
    Delta: number;
}

interface counterSummaryDto {
    Group: string;
    CounterName: string;
    Increments: number;
    Decrements: number;
    Total: number;
}

interface counterGroupDto {
    Name: string;
    Count: number;
}

interface counterServerValueDto {
    Name: string;
    Positive: number;
    Negative: number;
}

interface counterStorageReplicationDto {
    Destinations: counterStorageReplicationDestinatinosDto[];
}

interface counterStorageReplicationDestinatinosDto {
    Disabled: boolean;
    ServerUrl: string;
    CounterStorageName: string;
    Username: string;
    Password: string;
    Domain: string;
    ApiKey: string;
}

interface statusDebugChangesCounterStorageDto {
    WatchedChanges: Array<string>;
    WatchedLocalChanges: Array<string>;
    WatchedReplicationChanges: Array<string>;
    WatchedBulkOperationsChanges: Array<string>;
}

interface counterChangeNotification {
    GroupName: string;
    CounterName: string;
    CounterChangeAction: string;
    Delta: number;
    Total: number;
}

interface countersInGroupNotification extends counterChangeNotification {
}

interface counterBulkOperationNotificationDto{
    OperationId: string;
    BatchType: string;
    Message: string;
}