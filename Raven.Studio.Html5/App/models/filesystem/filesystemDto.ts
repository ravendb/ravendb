enum filesystemSynchronizationType {
    Unknown = 0,
    ContentUpdate = 1,
    MetadataUpdate = 2,
    Rename = 3,
    Delete = 4,
}

interface filesystemSynchronizationDetailsDto {
    Filename: string;
    FileETag: string;
    DestinationUrl: string;
    Type: filesystemSynchronizationType;
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

interface filesystemMetricsDto{
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
    Name: string;

    TotalSize?: number;
    UploadedSize: number;
		
    HumaneTotalSize: string;
    HumaneUploadedSize: string;

    Metadata: any;
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

interface filesystemSynchronizationReportDto{

    FileName: string;
    FileETag: string;
    Type: filesystemSynchronizationType;

    BytesTransfered: number;
    BytesCopied: number;
    NeedListLength: number;

    Exception: any;
}

interface filesystemSearchResultsDto {
    Files: filesystemFileHeaderDto[];
    FileCount: number;
    Start: number;
    PageSize: number;
}

interface filesystemConfigSearchResultsDto{
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

    RemoteHistory: filesystemHistoryItemDto[];
    CurrentHistory: filesystemHistoryItemDto[];
}

interface filesystemListPageDto<T> {
    TotalCount: number;
    Items: T[];
}
