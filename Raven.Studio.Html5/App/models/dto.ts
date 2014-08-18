interface collectionInfoDto extends indexResultsDto<documentDto> {
}

interface documentDto extends metadataAwareDto {

}

interface conflictsInfoDto extends indexResultsDto<conflictDto> {
}

interface dictionary<TValue> {
    [key: string]: TValue;
}

interface metadataAwareDto {
    '@metadata'?: documentMetadataDto;
}
interface documentChangeNotificationDto {
    Type: documentChangeType;
    Id: string;
    CollectionName: string;
    TypeName: string;
    Etag: string;
    Message: string;
}

interface bulkInsertChangeNotificationDto extends documentChangeNotificationDto{
    OperationId: string;
}

interface indexChangeNotificationDto {
    Type: indexChangeType;
    Name: string;
    Etag: string;
}

interface transformerChangeNotificationDto {
    Type: transformerChangeType;
    Name: string;
}

interface documentMetadataDto {
    'Raven-Entity-Name'?: string;
    'Raven-Clr-Type'?: string;
    'Non-Authoritative-Information'?: boolean;
    '@id'?: string;
    'Temp-Index-Score'?: number;
    'Last-Modified'?: string;
    'Raven-Last-Modified'?: string;
    '@etag'?: string;
}

interface databaseStatisticsDto {
    ApproximateTaskCount: number;
    CountOfDocuments: number;
    CountOfIndexes: number;
    CurrentNumberOfItemsToIndexInSingleBatch: number;
    CurrentNumberOfItemsToReduceInSingleBatch: number;
    DatabaseId: string;
    DatabaseTransactionVersionSizeInMB: number;
    Errors: serverErrorDto[];
    Extensions: Array<any>;
    InMemoryIndexingQueueSize: number;
    Indexes: indexStatisticsDto[];
    LastAttachmentEtag: string;
    LastDocEtag: string;
    Prefetches: Array<any>;
    StaleIndexes: Array<any>;
    ActualIndexingBatchSize: Array<any>;
    Triggers: Array<any>;
}

interface indexStatisticsDto {
    PublicName: string;
    IndexingAttempts: number;
    IndexingSuccesses: number;
    IndexingErrors: number;
    LastIndexedEtag: string;
    LastIndexedTimestamp: string;
    LastQueryTimestamp: string;
    TouchCount: number;
    Priority: string;
    ReduceIndexingAttempts: number;
    ReduceIndexingSuccesses: number;
    ReduceIndexingErrors: number;
    LastReducedEtag: string;
    LastReducedTimestamp: string;
    CreatedTimestamp: string;
    LastIndexingTime: string;
    IsOnRam: string; // Yep, really. Example values: "false", "true (3 KBytes)"
    LockMode: string;
    ForEntityName: string[];
    Performance: indexPerformanceDto[];
    DocsCount: number;
}

interface indexPerformanceDto {
    Operation: string;
    OutputCount: number;
    InputCount: number;
    ItemsCount: number;
    Duration: string;
    Started: string; // Date
    DurationMilliseconds: number;
}

interface apiKeyDto extends documentDto {
    Name: string;
    Secret: string;
    Enabled: boolean;
    Databases: Array<databaseAccessDto>;
}

interface serverBuildVersionDto {
    ProductVersion: string;
    BuildVersion: string;
}

interface clientBuildVersionDto {
    BuildVersion: string;
}

interface licenseStatusDto {
    Message: string;
    Status: string;
    Error: boolean;
    IsCommercial: boolean;
    ValidCommercialLicenseSeen: boolean;
    Attributes: {
        periodicBackup: string;
        encryption: string;
        compression: string;
        quotas: string;
        authorization: string;
        documentExpiration: string;
        replication: string;
        versioning: string;
        version: string;
        allowWindowsClustering: string;
        OEM: string;
        numberOfDatabases: string;
        maxSizeInMb: string;
        maxRamUtilization: string;
        maxParallelism: string;
    }
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

interface logDto {
    TimeStamp: string;
    Message: string;
    LoggerName: string;
    Level: string;
    Exception: string;
}

interface queryResultDto {
    Results: any[];
    Includes: any[];
}

interface alertContainerDto extends documentDto {
    Alerts: alertDto[];
}

interface alertDto {
    Title: string;
    CreatedAt: string;
    Observed: boolean;
    Message: string;
    AlertLevel: string;
    Exception: string;
    UniqueKey: string;
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
    MostRecentAttachmentEtag: string;
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

interface indexDefinitionContainerDto {
    Index: indexDefinitionDto;
}

interface indexDefinitionDto {
    Name: string;
    LockMode: string;
    Map: string;
    Maps: string[];
    Reduce: string;
    TransformResults: string;
    IsMapReduce: boolean;
    IsCompiled: boolean;
    Stores: any;
    Indexes: any;
    SortOptions: any;
    Analyzers: any;
    Fields: string[];
    Suggestions: any;
    TermVectors: any;
    SpatialIndexes: any; // This will be an object with zero or more properties, each property being the name of one of the .Fields, its value being of type spatialIndexDto.
    InternalFieldsMapping: any;
    Type: string;
    MaxIndexOutputsPerDocument;
}

/*
 * Represents a spatial field of an index. Shows up in the Edit Index view when the index has spatial fields defined.
*/
interface spatialIndexFieldDto {
    Type: string;
    Strategy: string;
    MaxTreeLevel: number;
    MinX: number;
    MaxX: number;
    MinY: number;
    MaxY: number;
    Units: string;
}

interface spatialIndexSuggestionDto {
    Distance: string;
    Accuracy: number;
}

interface periodicExportSetupDto {
    Disabled: boolean;
    GlacierVaultName: string;
    S3BucketName: string;
    AwsRegionEndpoint: string;
    AzureStorageContainer: string;
    LocalFolderName: string;
    IntervalMilliseconds: number;
    FullBackupIntervalMilliseconds: number;
}

interface indexResultsDto<T extends metadataAwareDto> {
    DurationMilliseconds: number;
    Highlightings: any;
    Includes: any;
    IndexEtag: string;
    IndexName: string;
    IndexTimestamp: string;
    IsStale: boolean;
    LastQueryTime: string;
    NonAuthoritativeInformation: boolean;
    ResultEtag: string;
    Results: T[];
    SkippedResults: number;
    TotalResults: number;
}

interface indexQueryResultsDto extends indexResultsDto<documentDto> {

}

interface versioningEntryDto extends documentDto {
  Id: string;
  MaxRevisions: number;
  Exclude: boolean;
}

interface versioningDto {
  Entries: versioningEntryDto[]
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
}

interface replicationsDto {
    Destinations: replicationDestinationDto[];
    Source: string;
}

interface replicationConfigDto {
    DocumentConflictResolution: string;
    AttachmentConflictResolution: string;
}

interface databaseAccessDto {
    Admin: boolean;
    TenantId: string;
    ReadOnly: boolean;
}

interface windowsAuthDataDto {
    Name: string;
    Enabled: boolean;
    Databases: databaseAccessDto[];
}

interface windowsAuthDto {
    RequiredGroups: windowsAuthDataDto[];
    RequiredUsers: windowsAuthDataDto[];
}

interface transformerDto {
    name: string;
    definition: {
        TransformResults: string;
        Name: string;
    }
}

interface saveTransformerDto {
    'Name': string;
    'TransformResults': string;
}

interface getTransformerResultDto {
    'Transformer': string;
}

interface savedTransformerDto {
    "Transformer":
    {
        "TransformResults": string;
        "Name": string;
    }
}

interface transformerParamInfo {
  name: string;
  hasDefault: boolean;
}

interface transformerParamDto {
    name: string;
    value: string;
}

interface transformerQueryDto {
    transformerName: string;
    queryParams: Array<transformerParamDto>;
}

interface storedQueryDto {
    IsPinned: boolean;
    IndexName: string;
    QueryText: string;
    Sorts: string[];
    TransformerQuery: transformerQueryDto;
    ShowFields: boolean;
    IndexEntries: boolean;
    UseAndOperator: boolean;
    Hash: number;
}

interface indexDataDto {
    name: string;
    hasReduce: boolean;
}

interface bulkDocumentDto {
    Key: string;
    Method: string;
    AdditionalData?: any[];
    Document?: documentDto; // Can be null when Method == "DELETE"
    Metadata?: documentMetadataDto; // Can be null when Method == "DELETE"
    Etag?: string; // Often is null on sending to server, non-null when returning from server.
    PatchResult?: any;
    Deleted?: any;
}

interface backupRequestDto {
    BackupLocation: string;
    DatabaseDocument: databaseDocumentDto;
}

interface backupStatusDto {
    Started: string;
    Completed?: string;
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
    RestoreLocation: string;
    DatabaseLocation: string;
    DatabaseName: string;
}

interface restoreStatusDto {
    Messages: string[];
    IsRunning: boolean;
}

interface sqlReplicationTableDto {
    TableName: string;
    DocumentKeyColumn: string;
}

interface sqlReplicationDto extends documentDto {
    Name: string;
    Disabled: boolean;
    ParameterizeDeletesDisabled: boolean;
    RavenEntityName: string;
    Script: string;
    FactoryName: string;
    ConnectionString: string;
    ConnectionStringName: string;
    PredefinedConnectionStringSettingName: string;
    ConnectionStringSettingName: string;
    SqlReplicationTables: sqlReplicationTableDto[];
    ForceSqlServerQueryRecompile?: boolean;
    PerformTableQuatation?:boolean;
}

interface commandData {
    CommandText: string;
    Params:{Key:string;Value:any}[]
}

interface tableQuerySummary {
    TableName: string;
    Commands: commandData[];
}

interface sqlReplicationSimulationResultDto {
    Results: tableQuerySummary[];
    LastAlert: alertDto;
}

interface sqlReplicationConnectionsDto extends documentDto {
    PredefinedConnections: predefinedSqlConnectionDto[];
}

interface predefinedSqlConnectionDto {
    Name:string;
    FactoryName: string;
    ConnectionString: string;
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

interface scriptedIndexDto extends documentDto {
    IndexScript: string;
    DeleteScript: string;
}

interface conflictDto extends documentDto {
    Id: string;
    ConflictDetectedAt: string;
    Versions: conflictVersionsDto[];
}

interface replicationSourceDto extends documentDto {
    LastDocumentEtag?: string;
    LastAttachmentEtag?: string;
    ServerInstanceId: string;
    Source: string;
}

interface conflictVersionsDto {
    Id: string;
    SourceId: string;
}

interface documentBase {
    getId(): string;
    getUrl(): string;
    getDocumentPropertyNames(): Array<string>;
}

interface smugglerOptionsDto {
    IncludeDocuments: boolean;
    IncludeIndexes: boolean;
    IncludeTransformers: boolean;
    IncludeAttachments: boolean;
    RemoveAnalyzers: boolean;
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

interface patchValueDto {
    Key: string;
    Value: string;
}

interface patchDto extends documentDto {
    PatchOnOption: string;
    Query: string;
    Script: string;
    SelectedItem: string;
    Values: Array<patchValueDto>;
}

interface statusDebugChangesDto {
    Id: string;
    Connected: boolean;
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
    Statistics: sqlReplicationStatisticsDto;
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
interface sqlReplicationStatisticsDto {
    Name: string;
    LastErrorTime: string;
    ScriptErrorCount: number;
    ScriptSuccessCount: number;
    WriteErrorCount: number;
    SuccessCount: number;
    LastAlert: alertDto;
}

interface statusDebugIndexFieldsDto {
    FieldNames: string[];
}

interface debugDocumentStatsDto {
    Total: number;
    TotalSize: number;
    Tombstones: number;
    System: number;
    SystemSize: number;
    NoCollection: number;
    NoCollectionSize: number;
    Collections: dictionary<collectionStats>;
    TimeToGenerate: string;
}

interface collectionStats {
    Quantity: number;
    Size: number;
}

enum documentChangeType {
    None = 0,
    Put = 1,
    Delete = 2,
    Common= 3,
    BulkInsertStarted = 4,
    BulkInsertEnded = 8,
    BulkInsertError = 16,
}

enum indexChangeType {
    None = 0,

    MapCompleted = 1,
    ReduceCompleted = 2,
    RemoveFromIndex = 4,

    IndexAdded = 8,
    IndexRemoved = 16,

    IndexDemotedToIdle = 32,
    IndexPromotedFromIdle = 64,

    IndexDemotedToAbandoned = 128,
    IndexDemotedToDisabled = 256,
    IndexMarkedAsErrored =  512
}

enum transformerChangeType {
    None = 0,
    TransformerAdded = 1,
    TransformerRemoved = 2
}

interface filterSettingDto {
    Path: string;
    Values: string[];
    ShouldMatch: boolean;
}

interface counterStorageDto {
    Name: string;
    Path?: string;
}

interface counterDto {
    Name: string;
    Group: string;
    OverallTotal: number;
    Servers: counterServerValueDto[];
}

interface counterGroupDto {
    Name: string;
    NumOfCounters?: number;
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

enum ImportItemType {
    Documents = 0x1,
    Indexes = 0x2,
    Attachments = 0x4,
    Transformers = 0x8,
    RemoveAnalyzers = 0x8000
}

interface changesApiEventDto {
    Time: string; // ISO date string
    Type: string;
    Value?: any;
}

interface databaseDto {
    Name: string;
    Disabled: boolean;
}

interface customFunctionsDto {
    Functions: string;
}

interface singleAuthToken {
    Token: string;
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

interface visualizerDataObjectNodeDto {
    children?: visualizerDataObjectNodeDto[];
    name?: string;
    level?: number;
    origin?: visualizerDataObjectNodeDto;
    x?: number;
    y?: number;
    depth?: number;
    parent?: visualizerDataObjectNodeDto;
    payload?: mappedResultInfo;
    connections?: visualizerDataObjectNodeDto[];
    cachedId?: string;
}

interface queryIndexDebugMapArgsDto {
    key?: string;
    sourceId?: string;
    startsWith?: string;
}

interface graphLinkDto {
    source: visualizerDataObjectNodeDto;
    target: visualizerDataObjectNodeDto;
    cachedId?: string;
}

interface mergeResult {
  Document: string;
  Metadata: string;
}

interface visualizerExportDto {
    indexName: string;
    docKeys: string[];
    reduceKeys: string[];
    tree: visualizerDataObjectNodeDto;
}

interface operationIdDto {
    OperationId: number;
}

interface operationStatusDto {
    Completed: boolean;
    State: documentStateDto[];
}

interface documentStateDto {
    Document: string;
    Deleted: boolean;
}

interface replicationTopologyDto {
    Servers: string[];
    Connections: replicationTopologyConnectionDto[];
}

interface replicationTopologyConnectionDto {
    Destination: string;
    DestinationToSourceState: string;
    Errors: string[];
    LastAttachmentEtag: string;
    LastDocumentEtag: string;
    ReplicationBehavior: string;
    SendServerId: string;
    Source: string;
    SourceToDestinationState: string;
    StoredServerId: string;
}

interface stringLinkDto {
    source: string;
    target: string;
}

interface replicationTopologyLinkDto extends stringLinkDto {
    left: boolean;
    right: boolean;
    toRightPayload?: replicationTopologyConnectionDto;
    toLeftPayload?: replicationTopologyConnectionDto;
}

interface runningTaskDto {
    Id: number;
    TaskStatus: string;
    Exception: string;
    ExceptionText: string;
    Payload: string;
    TaskType: string;
    StartTime: string;
}
