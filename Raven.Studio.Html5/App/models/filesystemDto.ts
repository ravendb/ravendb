enum filesystemSynchronizationType {
    Unknown = 0,
    ContentUpdate = 1,
    MetadataUpdate = 2,
    Rename = 3,
    Delete = 4,
}

interface filesystemSynchronizationDetails {
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
    ActiveSyncs: filesystemSynchronizationDetails[];
    PendingSyncs: filesystemSynchronizationDetails[];
}

interface filesystemFileHeaderDto {
    Name: string;

    TotalSize?: number;
    UploadedSize: number;
		
    HumaneTotalSize: string;
    HumaneUploadedSize: string;

    Metadata: any;
}