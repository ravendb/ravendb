/// <reference path="../../typescript/common/constants.ts"/>

interface collectionInfoDto extends Raven.Client.Documents.Queries.QueryResult<Array<documentDto>> {
}

interface logNotificationDto {
    Level: string;
    TimeStamp: string;
    LoggerName: string;
    RequestId: number;
    HttpMethod: string;
    ElapsedMilliseconds: number;
    ResponseStatusCode: number;
    RequestUri: string;
    TenantName: string;
    CustomInfo: string;
    InnerRequestsCount?: number;
    QueryTimings: any;

}

interface serverBuildVersionDto {
    BuildVersion: number;
    ProductVersion: string;
    CommitHash: string;
    FullVersion: string;
}

interface clientBuildVersionDto {
    Version: string;
}

interface supportCoverageDto {
    Status: string;
    EndsAt: string;
}

interface userInfoDto {
    Remark: string;
    User: string;
    IsAdminGlobal: boolean;
    IsAdminCurrentDb: boolean;
    Databases: string[];
    Principal: string;
    AdminDatabases: string[];
    ReadOnlyDatabases: string[];
    ReadWriteDatabases: string[];
    AccessTokenBody: string;
}

interface serverConfigsDto {
    IsGlobalAdmin: boolean;
    CanReadWriteSettings: boolean;
    CanReadSettings: boolean;
    CanExposeConfigOverTheWire: boolean;
}

interface logDto {
    TimeStamp: string;
    Message: string;
    Database: string;
    LoggerName: string;
    Level: string;
    Exception: string;
    StackTrace: string;
}

interface environmentColorDto {
    Name: string;
    BackgroundColor: string;
}

interface facetDto {
    Mode: number; // Default = 0, Ranges = 1
    Aggregation: number; // None = 0, Count = 1, Max = 2, Min = 4, Average = 8, Sum = 16
    AggregationField: string;
    AggregationType: string;
    Name: string;
    DisplayName: string;
    Ranges: any[];
    MaxResults: number;
    TermSortMode: number;
    IncludeRemainingTerms: boolean;
}

interface facetResultSetDto {
    Results: any; // An object containing keys that look like [FacetName]-[FacetAggregationField]. For example "Company-Total". Each key will be of type facetResultDto.
    Duration: string;
}

interface facetResultDto {
    Values: facetValueDto[];
    RemainingTerms: string[];
    RemainingTermsCount: number;
    RemainingHits: number;
}

interface facetValueDto {
    Range: string;
    Hits: number;
    Count: number;
    Sum: number;
    Max: number;
    Min: number;
    Average: number;
}

interface documentBase extends dictionary<any> {
    getId(): string;
    getUrl(): string;
    getDocumentPropertyNames(): Array<string>;
}

interface customColumnParamsDto {
    Header?: string;
    Binding: string;
    DefaultWidth?: number;
    Template?: string;
}

interface customColumnsDto {
    Columns: Array<customColumnParamsDto>;
}

interface statusDebugChangesDto {
    Id: string;
    Connected: boolean;
    DocumentStore: statusDebugChangesDocumentStoreDto;
    WatchAllDocuments: boolean;
    WatchAllIndexes: boolean;
    WatchConfig: boolean;
    WatchConflicts: boolean;
    WatchSync: boolean;
    WatchCancellations: boolean;
    WatchDocumentPrefixes: Array<string>;
    WatchDocumentsInCollection: Array<string>;
    WatchIndexes: Array<string>;
    WatchDocuments: Array<string>;
    WatchedFolders: Array<string>;
}


interface statusDebugChangesDocumentStoreDto {
    WatchAllDocuments: boolean;
    WatchAllIndexes: boolean;
    WatchAllReplicationConflicts: boolean;
    WatchedIndexes: Array<string>;
    WatchedDocuments: Array<string>;
    WatchedDocumentPrefixes: Array<string>;
    WatchedDocumentsInCollection: Array<string>;
    WatchedDocumentsOfType: Array<string>;
    WatchedBulkInserts: Array<string>;
}

interface statusDebugMetricsDto {
    DocsWritesPerSecond: number;
    IndexedPerSecond: number;
    ReducedPerSecond: number;
    RequestsPerSecond: number;
    Requests: meterDataDto;
    RequestsDuration: histogramDataDto;
    StaleIndexMaps: histogramDataDto;
    StaleIndexReduces: histogramDataDto;
    Gauges: any;
    ReplicationBatchSizeMeter: dictionary<meterDataDto>;
    ReplicationDurationMeter: dictionary<meterDataDto>;
    ReplicationBatchSizeHistogram: dictionary<histogramDataDto>;
    ReplicationDurationHistogram: dictionary<histogramDataDto>;
}

interface meterDataDto {
    Count: number;
    MeanRate: number;
    OneMinuteRate: number;
    FiveMinuteRate: number;
    FifteenMinuteRate: number;
}

interface histogramDataDto {
    Counter: number;
    Max: number;
    Min: number;
    Mean: number;
    Stdev: number;
    Percentiles: any;
}

interface statusDebugDocrefsDto {
    TotalCount: number;
    Results: Array<string>;
}

interface statusDebugIdentitiesDto {
    TotalCount: number;
    Identities: Array<{ Id: string; Value: string}>;
}

interface statusDebugCurrentlyIndexingDto {
    NumberOfCurrentlyWorkingIndexes: number;
    Indexes: Array<statusDebugIndexDto>;
}

interface statusDebugIndexDto {
    IndexName: string;
    IsMapReduce: boolean;
    RemainingReductions: number;
    CurrentOperations: Array<statusDebugIndexOperationDto>;
    Priority: string;
    OverallIndexingRate: Array<statusDebugIndexRateDto>;
}

interface statusDebugIndexOperationDto {
    Operation: string;
    NumberOfProcessingItems: number;
}

interface statusDebugIndexRateDto {
    Operation: string;
    Rate: string;
}

interface statusDebugQueriesGroupDto {
    IndexName: string;
    Queries: Array<statusDebugQueriesQueryDto>;
}

interface statusDebugQueriesQueryDto {
    StartTime: string;
    QueryInfo: string;
    QueryId: number;
    Duration: string;
}

interface taskMetadataDto {
    Id: any;
    IndexId: number;
    IndexName: string;
    AddedTime: string;
    Type: string;
}

interface taskMetadataSummaryDto {
    Type: string;
    IndexId: number;
    IndexName: string;
    Count: number;
    MinDate: string;
    MaxDate: string;
}

interface requestTracingDto {
    Uri: string;
    Method: string;
    StatusCode: number;
    RequestHeaders: requestHeaderDto[];
    ExecutionTime: string;
    AdditionalInfo: string;
}

interface requestHeaderDto {
    Name: string;
    Values: string[];
}

interface statusDebugIndexFieldsDto {
    FieldNames: string[];
}

interface debugDocumentStatsDto {
    Total: number;
    TotalSize: number;
    Tombstones: number;
    System: collectionStats;
    NoCollection: collectionStats;
    Collections: dictionary<collectionStats>;
    TimeToGenerate: string;
}

interface collectionStats {
    Stats: histogramDataDto;
    TotalSize: number;
    TopDocs: any[];
}

interface resourceStyleMap {
    resourceName: string;
    styleMap: any;
}

interface changesApiEventDto {
    Time: string; // ISO date string
    Type: string;
    Value?: any;
}

interface suggestionsDto {
    Suggestions: Array<string>;
}

interface queryFieldInfo {
    Index: number;
    FieldName: string;
    FieldValue: string;
}

interface mergeResult {
  Document: string;
  Metadata: string;
}

interface adminLogsConfigEntryDto {
    category: string;
    level: string;
    includeStackTrace: boolean;
}

interface performanceTestRequestDto {
    Path: string;
    FileSize: number;
    TestType: string;

    OperationType?: string;
    BufferingType?: string;
    Sequential?: boolean;
    ThreadCount?: number;
    TimeToRunInSeconds?: number;
    RandomSeed?: number;
    ChunkSize?: number;

    NumberOfDocuments?: number;
    SizeOfDocuments?: number;
    NumberOfDocumentsInBatch?: number;
    WaitBetweenBatches?: number;
}

interface diskPerformanceResultDto {
    ReadPerSecondHistory: number[];
    WritePerSecondHistory: number[];
    AverageReadLatencyPerSecondHistory: number[];
    AverageWriteLatencyPerSecondHistory: number[];
    ReadLatency: histogramDataDto;
    WriteLatency: histogramDataDto;
    TotalRead: number;
    TotalWrite: number;
    TotalTimeMs: number;
}

interface diskPerformanceResultWrappedDto {
    Result: diskPerformanceResultDto;
    Request: performanceTestRequestDto;
    DebugMsgs: string[];
}

interface pluginsInfoDto {
    Extensions: Array<extensionsLogDto>;
    Triggers: Array<triggerInfoDto>;
    CustomBundles: Array<string>;
}

interface extensionsLogDto {
    Name: string;
    Installed: Array<extensionsLogDetailDto>;
}

interface extensionsLogDetailDto {
    Name: string;
    Assembly: string;
}

interface triggerInfoDto {
    Type: string;
    Name: string;
}

interface dataExplorationRequestDto {
    Linq: string;
    Collection: string;
    TimeoutSeconds: number;
    PageSize: number;
}



interface performanceRunItemDto {
    displayName: string;
    documentId: string;
}
