interface collectionInfoDto {
	Results: documentDto[];
	Includes: any[];
	IsStale: boolean;
	IndexTimestamp: string;
	TotalResults: number;
	SkippedResults: number;
	IndexName: string; 
	IndexEtag: string;
	ResultEtag: string;
	Highlightings: any;
	NonAuthoritativeInformation: boolean;
	LastQueryTime: string;
	DurationMilliseconds: number;
}

interface documentDto {
	'@metadata'?: documentMetadataDto;
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

interface apiKeyDto {
    Name: string;
    Secret: string;
    Enabled: boolean;
    Databases: Array<apiKeyDatabaseDto>;
}

interface apiKeyDatabaseDto {
    TenantId: string;
    Admin: boolean;
    ReadOnly: boolean;
}

interface buildVersionDto {
    ProductVersion: string;
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

interface alertContainerDto {
    '@metadata': documentMetadataDto;
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

interface periodicBackupSetupDto {
    GlacierVaultName: string;
    S3BucketName: string;
    AwsRegionEndpoint: string;
    AzureStorageContainer: string;
    LocalFolderName: string;
    IntervalMilliseconds: number;
    FullBackupIntervalMilliseconds: number;
}

interface indexQueryResultsDto {
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
    Results: documentDto[];
    SkippedResults: number;
    TotalResults: number;
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

interface transformerDto {
    name: string;
    definition: {
        TransformResults: string;
        Name: string;
    }
}

interface storedQueryDto {
    IsPinned: boolean;
    IndexName: string;
    QueryText: string;
    Sorts: string[];
    TransformerName: string;
    ShowFields: boolean;
    IndexEntries: boolean;
    UseAndOperator: boolean;
}

interface storedQueryContainerDto extends documentDto {
    Queries: storedQueryDto[];
}