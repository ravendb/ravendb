/// <reference path="../../typescript/common/constants.ts"/>

interface filesystemSynchronizationDetailsDto {
    FileName: string;
    FileETag: string;
    DestinationUrl: string;
    Type: string;
    Direction: synchronizationDirection;
}

interface filesystemMetricsHistogramDataDto {
    Counter: number;
    Max: number;
    Min: number;
    Mean: number;
    Stdev: number;
}

interface filesystemMetricsMeterDataDto {
    Count: number;
    MeanRate: number;
    OneMinuteRate: number;
    FiveMinuteRate: number;
    FifteenMinuteRate: number;
}

interface filesystemMetricsDto {
    FilesWritesPerSecond: number;
    RequestsPerSecond: number;
    Requests: filesystemMetricsMeterDataDto;
    RequestsDuration: filesystemMetricsHistogramDataDto;
}

interface filesystemStatisticsDto {
    Name: string;
    FileCount: number;
    Metrics: filesystemMetricsDto;
    ActiveSyncs: filesystemSynchronizationDetailsDto[];
    PendingSyncs: filesystemSynchronizationDetailsDto[];
}

interface filesystemFileHeaderDto {
    CreationDate: Date;
    Directory: string;
    Etag: string;
    Extension: string;
    FullPath: string;
    HumaneTotalSize: string;
    LastModified: Date;
    Metadata: any;
    Name: string;
    OriginalMetadata: any;
    TotalSize?: number;
    UploadedSize: number;

    HumaneUploadedSize: string;   
}

interface fileMetadataDto {
    'Last-Modified'?: string;
    '@etag'?: string;
    'Raven-Synchronization-History': string;
    'Raven-Synchronization-Version': string;
    'Raven-Synchronization-Source': string;
    'RavenFS-Size': string;
    'Origin': string;
}

interface filesystemSearchResultsDto {
    Files: filesystemFileHeaderDto[];
    FileCount: number;
    Start: number;
    PageSize: number;
}

interface filesystemConfigSearchResultsDto {
    ConfigNames: string[];
    TotalCount: number;
    Start: number;
    PageSize: number;
}

interface filesystemHistoryItemDto {
    Version: number;
    ServerId: string;
}

interface filesystemConflictItemDto {
    FileName: string;
    RemoteServerUrl: string;
    ResolveUsingRemote: boolean;

    RemoteHistory: filesystemHistoryItemDto[];
    CurrentHistory: filesystemHistoryItemDto[];
}

interface filesystemListPageDto<T> {
    TotalCount: number;
    Items: T[];
}

interface synchronizationReplicationsDto {
    Destinations: synchronizationDestinationDto[];
    Source: string;
}

interface synchronizationDestinationDto {
    ServerUrl: string;
    Username: string;
    Password: string;
    Domain: string;
    ApiKey: string;
    FileSystem: string;
    Enabled: boolean;
}

interface folderNodeDto {
    key: string;
    title: string;
    isLazy: boolean;
    isFolder: boolean
    addClass?: string;
}

interface synchronizationUpdateNotification {
    FileSystemName: string;
    FileName: string;
    DestinationFileSystemUrl: string;
    SourceServerId: string;
    SourceFileSystemUrl: string;
    Type: filesystemSynchronizationType;
    Action: string;
    Direction: string;
}


interface synchronizationConflictNotification {
    FileSystemName: string;
    FileName: string;
    SourceServerUrl: string;
    Status: string;
    RemoteFileHeader: any;
}

interface fileChangeNotification {
    FileSystemName: string;
    File: string;
    Action: string;
}

interface filesystemConfigNotification {
    FileSystemName: string;
    Name: string;
    Action: filesystemConfigurationChangeAction;
}


