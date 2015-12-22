interface counterStorageStatisticsDto {
    Name: string;
    Url: string;
    CountersCount: number;
    GroupsCount: number;
    LastCounterEtag:string;
    ReplicationTasksCount: number;
    CounterStorageSize: string;
    RequestsPerSecond: number;
}

interface counterStorageDto {
    Name: string;
    Path?: string;
}

interface counterTotalDto {
    CurrentValue: number;
    Group: string;
    CounterName: string;
    Delta: number;
}

interface counterSummaryDto {
    GroupName: string;
    CounterName: string;
    Total: number;
}

interface counterGroupDto {
    Name: string;
    Count: number;
}

interface counterDto {
    ServerValues: serverValueDto[];
    LocalServerId: string;
    LastUpdateByServer: string;
    Total: number;
    NumOfServers: number;
}

interface serverValueDto {
    ServerId: string;
    Value: number;
    Etag: number;
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

interface IGroupAndCounterName {
    groupName: string;
    counterName: string;
}
