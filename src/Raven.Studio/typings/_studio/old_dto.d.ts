/// <reference path="../../typescript/common/constants.ts"/>

interface collectionInfoDto extends Raven.Client.Documents.Queries.QueryResult<Array<documentDto>> {
}

interface conflictsInfoDto extends Raven.Client.Documents.Queries.QueryResult<Array<conflictDto>> {
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

interface apiKeyDto extends documentDto {
    Name: string;
    Secret: string;
    Enabled: boolean;
    Databases: Array<databaseAccessDto>;
}

interface serverBuildVersionDto {
    BuildVersion: number;
    ProductVersion: string;
    CommitHash: string;
    FullVersion: string;
}

interface latestServerBuildVersionDto {
    LatestBuild: number;
    Exception: string;
}

interface clientBuildVersionDto {
    Version: string;
}

interface supportCoverageDto {
    Status: string;
    EndsAt: string;
}

interface HotSpareDto {
    ActivationMode: string;
    ActivationTime: string;
    RemainingTestActivations: number;
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

interface serverErrorDto {
    Index: number;
    IndexName: string;
    Error: string;
    Timestamp: string;
    Document: string;
    Action: string;
}

interface replicationStatsDocumentDto {
    Self: string; // e.g. "http://judah-pc:8080/databases/ReplSrc"
    MostRecentDocumentEtag: string;
    Stats: replicationStatsDto[];
}

interface replicationStatsDto {
    FailureCountInternal: number;
    Url: string;
    LastHeartbeatReceived: string;
    LastEtagCheckedForReplication: string;
    LastReplicatedEtag: string;
    LastReplicatedLastModified: string;
    LastSuccessTimestamp: string;
    LastFailureTimestamp: string;
    FailureCount: number;
    LastError: string;
}

interface documentCountDto {
    Count: number;
    Type: string;
    IsEtl: boolean;
}

interface replicationDestinationDto {
    Url: string;
    Username: string;
    Password: string;
    Domain: string;
    ApiKey: string;
    Database: string;
    TransitiveReplicationBehavior: string;
    IgnoredClient: boolean;
    Disabled: boolean;
    ClientVisibleUrl: string;
    SpecifiedCollections: dictionary<string>;
    HasGlobal?: boolean;
    HasLocal?: boolean;
}

interface configurationDocumentDto<TClass> {
    LocalExists?: boolean;
    GlobalExists?: boolean;
    MergedDocument: TClass;
    GlobalDocument?: TClass;
    Etag?: string;
    Metadata?: any;
}

interface configurationSettingDto {
    LocalExists: boolean;
    GlobalExists: boolean;
    EffectiveValue: string;
    GlobalValue: string;
}

interface configurationSettingsDto {
    Results: dictionary<configurationSettingDto>;
}

interface replicationsDto {
    Destinations: replicationDestinationDto[];
    Source: string;
    ClientConfiguration?: replicationClientConfigurationDto;
}

interface replicationClientConfigurationDto {
    FailoverBehavior?: string;
    RequestTimeSlaThresholdInMilliseconds: number;
}

interface environmentColorDto {
    Name: string;
    BackgroundColor: string;
}

interface replicationConfigDto {
    DocumentConflictResolution: string;    
}

interface databaseAccessDto {
    Admin: boolean;
    TenantId: string;
    ReadOnly: boolean;
}

interface storedPatchDto extends patchDto {
    Hash: number;
}

interface scriptedPatchRequestDto {
    Script: string;
    Values: any;
}

interface databaseDocumentSaveDto {
    Key: string;
    ETag: number;
}

interface backupRequestDto {
    BackupLocation: string;
    DatabaseDocument: databaseDocumentDto;
}

interface backupStatusDto {
    Started: string;
    Completed?: string;
    Success?: string;
    IsRunning: boolean;
    Messages: backupMessageDto[];
}

interface backupMessageDto {
    Message: string;
    Timestamp: string;
    Severity: string;
}

interface databaseDocumentDto {
    Id: string;
    Settings: {};
    SecuredSettings: {};
    Disabled: boolean;
}

interface restoreRequestDto {
    BackupLocation: string;
    IndexesLocation: string;
    JournalsLocation: string;
}

interface databaseRestoreRequestDto extends restoreRequestDto {
    DatabaseName: string;
    DatabaseLocation: string;
    DisableReplicationDestinations: boolean;
    GenerateNewDatabaseId?: boolean;
}

interface restoreStatusDto {
    Messages: string[];
    State: string;
}

interface compactStatusDto {
    Messages: string[];
    LastProgressMessage: string;
    State: string;
}

interface commandData {
    CommandText: string;
    Params:{Key:string;Value:any}[];
}

interface tableQuerySummary {
    TableName: string;
    Commands: commandData[];
}

interface sqlReplicationSimulationResultDto {
    Results: tableQuerySummary[];
    //TODO: LastAlert: alertDto;
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

interface conflictDto extends documentDto {
    Id: string;
    ConflictDetectedAt: string;
    Versions: conflictVersionsDto[];
}

interface replicationSourceDto extends documentDto {
    LastDocumentEtag?: string;
    ServerInstanceId: string;
    Source: string;
}

interface conflictVersionsDto {
    Id: string;
    SourceId: string;
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

interface statusStorageOnDiskDto {
    TransactionalStorageAllocatedSize: number;
    TransactionalStorageAllocatedSizeHumaneSize: string;
    TransactionalStorageUsedSize: number;
    TransactionalStorageUsedSizeHumaneSize: string;
    IndexStorageSize: number;
    IndexStorageSizeHumane: string;
    TotalDatabaseSize: number;
    TotalDatabaseSizeHumane: string;
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
    WatchAllTransformers: boolean;
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
    Identities: Array<{ Key: string; Value: string}>;
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

interface sqlReplicationStatsDto {
    Name: string;
    Statistics: any;
    Metrics: sqlReplicaitonMetricsDto;
}
interface sqlReplicaitonMetricsDto {
    GeneralMetrics: dictionary<metricsDataDto>;
    TablesMetrics: dictionary<dictionary<metricsDataDto>>;
}
interface metricsDataDto {
    Type: string;
    Name:string;
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

interface databaseDto extends tenantDto {
    IndexingDisabled: boolean;
    RejectClientsEnabled: boolean;
    ClusterWide: boolean;
}

interface tenantDto {
    IsLoaded: boolean;
    Name: string;
    Disabled: boolean;
    Bundles: Array<string>;
    IsAdminCurrentTenant: boolean;
}

interface suggestionsDto {
    Suggestions: Array<string>;
}

interface queryFieldInfo {
    Index: number;
    FieldName: string;
    FieldValue: string;
}

interface indexSuggestion extends queryFieldInfo {
    Suggestion: string;
}

interface mappedResultInfo {
    ReduceKey?: string;
    Timestamp?: string;
    Etag?: string;
    Data?: any;
    Bucket?: number;
    Source?: string;
}


interface visualizerDataObjectDto {
    x?: number;
    y?: number;
    type: number;
    id: any;
    source?: any;
    idx: number;
}

interface queryIndexDebugMapArgsDto {
    key?: string;
    sourceId?: string;
    startsWith?: string;
}

interface mergeResult {
  Document: string;
  Metadata: string;
}

interface operationStatusDto {
    Completed: boolean;
    Faulted: boolean;
    Canceled: boolean;
    State: operationStateDto;
}

interface operationStateDto {
    Error?: string;
    Progress?: string;
}

interface bulkOperationStatusDto extends operationStatusDto {
    OperationProgress: bulkOperationProgress;
}

interface internalStorageBreakdownState extends operationStatusDto {
    ReportResults: string[];
}

interface debugDocumentStatsStateDto extends operationStatusDto {
    Stats: debugDocumentStatsDto;
}

interface documentStateDto {
    Document: string;
    Deleted: boolean;
}

interface bulkOperationProgress {
    TotalEntries: number;
    ProcessedEntries: number;
}


interface dataDumperOperationStatusDto extends operationStatusDto {
    ExceptionDetails: string;
}

interface importOperationStatusDto extends operationStatusDto{
    LastProgress: string;
    ExceptionDetails: string;
}

interface globalTopologyDto {
    Databases: replicationTopologyDto;
}

interface replicationTopologyDto {
    Servers: string[];
    Connections: replicationTopologyConnectionDto[];
    SkippedResources: string[];
}

interface synchronizationTopologyDto {
    Servers: string[];
    Connections: synchronizationTopologyConnectionDto[];
    SkippedResources: string[];
}

interface replicationTopologyConnectionDto {
    Destination: string;
    DestinationToSourceState: string;
    Errors: string[];
    LastDocumentEtag: string;
    ReplicationBehavior: string;
    SendServerId: string;
    Source: string;
    SourceToDestinationState: string;
    StoredServerId: string;
    UiType: string;
}

interface synchronizationTopologyConnectionDto {
    Destination: string;
    DestinationToSourceState: string;
    Errors: string[];
    LastSourceFileEtag: string;
    SendServerId: string;
    Source: string;
    SourceToDestinationState: string;
    StoredServerId: string;
    UiType: string;
}

interface runningTaskDto {
    Id: number;
    Status: operationStateDto;
    Exception: string;
    Killable: boolean;
    Completed: boolean;
    Faulted: boolean;
    Canceled: boolean;
    Description: string;
    TaskType: string;
    StartTime: string;
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

interface indexReplaceDocumentDto extends documentDto {
    IndexToReplace: string;
    MinimumEtagBeforeReplace?: string;
    ReplaceTimeUtc?: string;
}

interface replicationExplanationForDocumentDto {
    Key: string;
    Etag: string;
    Destination: destinationInformationDto;
    Message: string;
}

interface destinationInformationDto {
    Url: string;
    DatabaseName: string;
    ServerInstanceId: string;
    LastDocumentEtag: string;
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

interface copyFromParentDto<T> {
    copyFromParent(parent: T): void;
}
interface topologyDto {
    CurrentLeader: string;
    CurrentTerm: number;
    State: string;
    CommitIndex: number;
    AllVotingNodes: Array<nodeConnectionInfoDto>;
    PromotableNodes: Array<nodeConnectionInfoDto>;
    NonVotingNodes: Array<nodeConnectionInfoDto>;
    TopologyId: string;
}

interface nodeConnectionInfoDto {
    Uri: string;
    Name: string;
    Username?: string;
    Password?: string;
    Domain?: string;
    ApiKey?: string;
    IsNoneVoter?: boolean;
}

interface clusterConfigurationDto {
    EnableReplication: boolean;
    DatabaseSettings?: dictionary<string>;
}

interface clusterNodeStatusDto {
    Uri: string;
    Status: string;
}

interface serverSmugglingItemDto {
    Name: string;
    Incremental: boolean;
}

interface serverConnectionInfoDto {
    Url: string;
    Username: string;
    Password: string;
    Domain: string;
    ApiKey: string;
}

interface serverSmugglingDto {
    TargetServer: serverConnectionInfoDto;
    Config: Array<serverSmugglingItemDto>;
}

interface serverSmugglingOperationStateDto extends operationStatusDto {
    Messages: Array<string>;
}

interface dataExplorationRequestDto {
    Linq: string;
    Collection: string;
    TimeoutSeconds: number;
    PageSize: number;
}

interface adminJsScriptDto {
    Script: string;
}


interface consoleJsSampleDto {
    Name: string;
    Code: string;
}

interface diskIoPerformanceRunDto {
    ProcessId: number;
    ProcessName: string;
    DurationInMinutes: number;
    StartTime: string;
    Databases: Array<diskIoPerformanceRunResultDto>;
}

interface diskIoPerformanceRunResultDto
{
    Name: string;
    Results: dictionary<Array<diskIoPerformanceRunIoResultDto>>;
}

interface diskIoPerformanceRunIoResultDto extends documentDto {
    PathType: string;
    WriteDurationInMilliseconds: number;
    WriteIoSizeInBytes: number;
    ReadDurationInMilliseconds: number;
    ReadIoSizeInBytes: number;
    NumberOfReadOperations: number;
    NumberOfWriteOperations: number;
}

interface performanceRunItemDto {
    displayName: string;
    documentId: string;
}

interface generatedCodeDto {
    Document: string;
    Code: string;
}