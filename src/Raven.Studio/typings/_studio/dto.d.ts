/// <reference path="../tsd.d.ts"/>

interface disposable {
    dispose(): void;
}

interface dictionary<TValue> {
    [key: string]: TValue;
}

interface valueAndLabelItem<V, L> {
    value: V;
    label: L;
}

interface queryResultDto<T> {
    Results: T[];
    Includes: any[];
}

interface changesApiEventDto {
    Time: string; // ISO date string
    Type: string;
    Value?: any;
}

interface resultsDto<T> {
    Results: T[];
}

interface statusDto<T> {
    Status: T[];
}

interface resultsWithTotalCountDto<T> extends resultsDto<T> {
    TotalResults: number;
}

interface resultsWithCountAndToken<T> extends resultsWithTotalCountDto<T> {
    ContinuationToken?: string;
}
interface resultsWithCountScannedResultsAndToken<T> extends resultsWithTotalCountDto<T> {
    ContinuationToken?: string;
    ScannedResults?: number;
}
interface resultsWithCountAndAvailableColumns<T> extends resultsWithCountAndToken<T> {
    AvailableColumns: string[];
}

interface resultsWithCountAndAvailableColumns<T> extends resultsWithCountAndToken<T> {
    AvailableColumns: string[];
}

interface documentDto extends metadataAwareDto {
    [key: string]: any;
}

interface metadataAwareDto {
    '@metadata'?: documentMetadataDto;
}

interface changeVectorItem {
    fullFormat: string;
    shortFormat: string;
}

interface IndexErrorPerDocument {
    Document: string;
    Error: string;
    Action: string;
    IndexName: string;
    Timestamp: string;
    LocalTime: string;
    RelativeTime: string;
}

interface revisionTimeSeriesDto {
    Count: number;
    Start: string;
    End: string;
}

interface documentMetadataDto {
    '@collection'?: string;
    'Raven-Clr-Type'?: string;
    'Non-Authoritative-Information'?: boolean;
    '@id': string;
    'Temp-Index-Score'?: number;
    '@last-modified'?: string;
    '@flags'?: string;
    '@attachments'?: Array<documentAttachmentDto>;
    '@change-vector'?: string;
    '@shard-number'?: number;
    '@counters'?: Array<string>;
    '@counters-snapshot'?: dictionary<number>;
    '@timeseries-snapshot'?: dictionary<revisionTimeSeriesDto>;
    '@timeseries'?: Array<string>;
    '@expires'?: string;
    '@refresh'?: string;
    '@archive-at'?: string;
    '@archived'?: boolean;
}

interface updateDatabaseConfigurationsResult {
    RaftCommandIndex: number;
}

interface updateConflictSolverConfigurationResponse extends updateDatabaseConfigurationsResult {
    ConflictSolverConfig: Raven.Client.ServerWide.ConflictSolver;
}

interface nodeCounterValue {
    nodeTag: string;
    databaseId: string;
    nodeCounterValue: number;
}

interface counterItem {
    documentId: string;
    counterName: string;
    totalCounterValue: number;
    counterValuesPerNode: Array<nodeCounterValue>;
}

interface attachmentItem {
    documentId: string;
    name: string;
    contentType: string;
    size: number;
}

interface timeSeriesItem {
    name: string;
    numberOfEntries: number;
    startDate: string;
    endDate: string;
}

interface timeSeriesPlotItem {
    documentId: string;
    name: string;
    value: timeSeriesQueryResultDto;
}

type timeSeriesDeleteMode = "all" | "range" | "selection";

interface timeSeriesDeleteCriteria {
    mode: timeSeriesDeleteMode;
    selection?: Raven.Client.Documents.Session.TimeSeriesValue[];
}

type postTimeSeriesDeleteAction = "reloadCurrent" | "changeTimeSeries" | "doNothing";

interface filterTimeSeriesDates<T> {
    startDate: T;
    endDate: T;
}

interface documentAttachmentDto {
    ContentType: string;
    Hash: string;
    Name: string;
    Size: number;
}

interface connectedDocument {
    id: string;
    href: string;
}

interface canActivateResultDto {
    redirect?: string;
    can?: boolean;
}

interface canDeactivateResultDto {
    can?: boolean;
}

interface confirmDialogResult {
    can: boolean;
}

interface disableDatabaseResult {
    Name: string;
    Success: boolean;
    Reason: string;
    Disabled: boolean;
}

interface deleteDatabaseConfirmResult extends confirmDialogResult {
    keepFiles: boolean;
}

interface backupNowConfirmResult extends confirmDialogResult {
    isFullBackup: boolean;
}

type menuItemType = "separator" | "intermediate" | "leaf" | "collections";

interface menuItem {
    disableWithReason?: KnockoutObservable<string>;
    type: menuItemType;
    parent: KnockoutObservable<menuItem>;
}

interface menuSearchConfig {
    alternativeTitles?: string[];
    innerActions?: innerMenuAction[];
    overrideTitle?: string;
    isExcluded?: boolean;
    isCapitalizedDisabled?: boolean;
}

interface innerMenuAction {
    name: string;
    alternativeNames?: string[];
}

type dynamicHashType = KnockoutObservable<string> | (() => string);

type shardingMode = "allShards" | "singleShard";

interface chagesApiConfigureRequestDto {
    Command: string;
    Param?: string;
}

interface changedOnlyMetadataFieldsDto extends documentMetadataDto {
    Type: string;
    RevisionCreated: boolean;
}

interface saveDocumentResponseDto {
    Results: Array<changedOnlyMetadataFieldsDto>;
}

interface operationIdResults {
    Results: operationIdDto[];
}

interface operationIdDto {
    OperationId: number;
}

// Matching interface from Cloud Project 
interface federatedCredentials extends Raven.Client.Documents.Operations.Backups.S3Settings, IBackupCredentials {
}

interface AzureSasCredentials extends IBackupCredentials {
    StorageContainer: string;
    RemoteFolderName: string;
    AccountName: string;
    SasToken: string;
}

type BackupStorageType = "S3" | "Azure";

interface IBackupCredentials {
    BackupStorageType: BackupStorageType;
    Expires: string;
}

interface storageReportDto {
    BasePath: string;
    Results: storageReportItemDto[];
}

interface storageReportItemDto {
    Name: string;
    Type: string;
    Report: Voron.Debugging.StorageReport;
}

interface detailedSystemStorageReportItemDto {
    Environment: string;
    Type: string;
    Report: Voron.Debugging.DetailedStorageReport;
}

interface detailedStorageReportItemDto {
    Name: string;
    Type: string;
    Report: Voron.Debugging.DetailedStorageReport;
}

interface arrayOfResultsAndCountDto<T> {
    Results: T[];
    Count: number;
}

interface subscriptionItemTooltipInfo {
    title: string;
    clientUri: string;
}

interface subscriptionConnectionItemInfo extends subscriptionItemTooltipInfo {
    duration: number;
    strategy: string;
    batchCount: number;
    totalBatchSize: number;
    connectionId: number;
    exceptionText: string;
}

interface subscriptionPendingItemInfo extends subscriptionItemTooltipInfo {
    duration: number;
}

interface subscriptionErrorItemInfo extends subscriptionItemTooltipInfo {
    exceptionText: string;
    strategy: string;
}

interface timeGapInfo {
    durationInMillis: number;
    start: Date;
}
interface documentColorPair {
    docName: string;
    docColor: string;
}

interface workData {
    pointInTime: number;
    numberOfItems: number;
}

interface workTimeUnit {
    startTime: number;
    endTime: number;
}

interface queryDto {
    name: string;
    queryText: string;
    modificationDate: string;
    recentQuery: boolean;
}

interface storedQueryDto extends queryDto {
    hash: number;
}

interface replicationConflictListItemDto {
    Id: string;
    LastModified: string;
    ConflictsPerDocument: number;
}

type databaseDisconnectionCause = "Error" | "DatabaseDeleted" | "DatabaseDisabled" | "ChangingDatabase" | "DatabaseIsNotRelevant" | "DatabaseRestarted";

type querySortType = "Ascending" | "Descending" | "Range Ascending" | "Range Descending";

interface recentErrorDto extends Raven.Server.NotificationCenter.Notifications.Notification {
    Details: string;
    HttpStatus?: string;
}

declare module studio.settings {
    type numberFormatting = "raw" | "formatted";
    type dontShowAgain = "UnsupportedBrowser";
    type saveLocation = "local" | "remote";
}

interface IndexingPerformanceStatsWithCache extends Raven.Client.Documents.Indexes.IndexingPerformanceStats {
    StartedAsDate: Date;
    StartedAsDateExcludingWaitTime: Date;
    WaitOperation: Raven.Client.Documents.Indexes.IndexingPerformanceOperation;
    CompletedAsDate: Date;
    DetailsExcludingWaitTime: Raven.Client.Documents.Indexes.IndexingPerformanceOperation;
}

interface IOMetricsRecentStatsWithCache extends Raven.Server.Utils.IoMetrics.IOMetricsRecentStats {
    StartedAsDate: Date; // used for caching
    CompletedAsDate: Date; // used for caching
}

type subscriptionType =  "SubscriptionConnection" | "SubscriptionBatch" | "AggregatedBatchesInfo";

type ongoingTaskStatType = Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType | StudioEtlType | subscriptionType | StudioQueueSinkType;

interface ReplicationPerformanceBaseCache {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType;
    Description: string;
    HasErrors: boolean;
}

type OutgoingReplicationPerformanceWithCache = Raven.Client.Documents.Replication.OutgoingReplicationPerformanceStats & ReplicationPerformanceBaseCache;
type IncomingReplicationPerformanceWithCache = Raven.Client.Documents.Replication.IncomingReplicationPerformanceStats & ReplicationPerformanceBaseCache;
type ReplicationPerformanceWithCache = OutgoingReplicationPerformanceWithCache | IncomingReplicationPerformanceWithCache;

interface EtlPerformanceBaseWithCache extends Raven.Server.Documents.ETL.Stats.EtlPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: StudioEtlType;
    HasErrors: boolean;
    HasLoadErrors: boolean;
    HasTransformErrors: boolean;
}

interface QueueSinkPerformanceBaseWithCache extends Raven.Server.Documents.QueueSink.Stats.Performance.QueueSinkPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    HasErrors: boolean;
    HasScriptErrors: boolean;
    HasReadErrors: boolean;
    Type: StudioQueueSinkType;
}

interface SubscriptionConnectionPerformanceStatsWithCache extends Raven.Server.Documents.Subscriptions.Stats.SubscriptionConnectionPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: subscriptionType;
    HasErrors: boolean;
}

interface SubscriptionBatchPerformanceStatsWithCache extends Raven.Server.Documents.Subscriptions.Stats.SubscriptionBatchPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: subscriptionType;
    HasErrors: boolean;
    AggregatedBatchesCount: number;
}

interface IndexingPerformanceOperationWithParent extends Raven.Client.Documents.Indexes.IndexingPerformanceOperation {
    Parent: Raven.Client.Documents.Indexes.IndexingPerformanceStats;
}

interface disabledReason {
    disabled: boolean;
    reason?: string;
}

interface pagedResult<T> {
    items: T[];
    totalResultCount: number;
    resultEtag?: string;
    additionalResultInfo?: any; 
}

interface testSubscriptionPagedResult<T> extends pagedResult<T> {
    includes: dictionary<any>;
}

interface pagedResultExtended<T> extends pagedResult<T> {
    includes: dictionary<any>;
    includesRevisions?: Array<any>;
    highlightings?: dictionary<dictionary<Array<string>>>;
    explanations?: dictionary<Array<string>>;
    timings?: Raven.Client.Documents.Queries.Timings.QueryTimings;
    queryPlan?: Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
}

interface pagedResultWithToken<T> extends pagedResult<T> {
    continuationToken?: string;
}

interface pagedResultWithTokenAndSkippedResults<T> extends pagedResult<T> {
    continuationToken?: string;
    scannedResults?: number;
}

interface pagedResultWithAvailableColumns<T> extends pagedResultWithToken<T> {
    availableColumns: string[];
}

type clusterNodeType = "Member" | "Promotable" | "Watcher";
type databaseGroupNodeType = "Member" | "Promotable" | "Rehab";
type subscriptionStartType = 'Beginning of Time' | 'Latest Document' | 'Change Vector';

interface patchDto {
    Name: string;
    Query: string;
    RecentPatch: boolean;
    ModificationDate: string;
}

interface storedPatchDto extends patchDto {
    Hash: number;
}

interface feedbackSavedSettingsDto {
    Name: string;
    Email: string;
}

interface layoutable {
    x: number;
    y: number;
}

interface indexStalenessReasonsResponse {
    IsStale: boolean;
    StalenessReasons: string[];
}

interface autoCompleteWordList {
    caption: string; 
    value: string; 
    snippet?: string; 
    score: number; 
    meta: string;
    completer?: {
        insertMatch(editor: AceAjax.Editor, data: autoCompleteWordList);
    }
}

interface autoCompleteLastKeyword {
    info: rqlQueryInfo, 
    keyword: string,
    asSpecified: boolean,
    notSpecified: boolean,
    binaryOperation: string,
    whereFunction: string,
    whereFunctionParameters: number,
    fieldPrefix: string[],
    fieldName: string,
    dividersCount: number,
    parentheses: number
}

interface rqlQueryInfo {
    collection: string;
    index: string;
    alias: string;
    aliases: dictionary<string>;
}

interface queryCompleterProviders {
    indexFields: (indexName: string, callback: (fields: string[]) => void) => void;
    collectionFields: (collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void) => void;
    collections: (callback: (collectionNames: string[]) => void) => void;
    indexNames: (callback: (indexNames: string[]) => void) => void;
}

type rqlQueryType = "Select" | "Update";

type autoCompleteCompleter = (editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: autoCompleteWordList[]) => void) => void;
type certificateMode = "generate" | "regenerate" | "upload" | "editExisting" | "replace";

interface unifiedCertificateDefinition extends Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition {
    Thumbprints: Array<string>;
    Visible: KnockoutObservable<boolean>;
    HasTwoFactor: boolean;
    LastUsed: KnockoutObservable<string>;
}

type dashboardChartTooltipProviderArgs = {
    date: Date;
    values: dictionary<number>;
}

interface documentBase extends dictionary<any> {
    getId(): string;
    getUrl(): string;
    getDocumentPropertyNames(): Array<string>;
}

interface domainAvailabilityResult {
    Available: boolean;
    IsOwnedByMe: boolean;
}

interface collectionInfoDto extends Raven.Client.Documents.Queries.QueryResult<Array<documentDto>, any> {
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

interface resourceStyleMap {
    resourceName: string;
    styleMap: any;
}

type checkbox = "unchecked" | "some_checked" | "checked";

type sqlMigrationAction = "skip" | "embed" | "link";

interface sqlMigrationAdvancedSettingsDto {
    UsePascalCase: boolean,
    TrimSuffix: boolean,
    SuffixToTrim: string,
    DetectManyToMany: boolean 
}

type virtualNotificationType = 
    "CumulativeBulkInsert" 
    | "CumulativeBulkInsertFailures" 
    | "AttachmentUpload" 
    | "CumulativeUpdateByQuery" 
    | "CumulativeUpdateByQueryFailures" 
    | "CumulativeDeleteByQuery"
    | "CumulativeDeleteByQueryFailures";

declare module Raven.Server.NotificationCenter.Notifications {
    interface Notification {
        // extend server side type to contain local virtual notifications 
        Type: Raven.Server.NotificationCenter.Notifications.NotificationType | virtualNotificationType;
    }
}

interface explainQueryResponse extends resultsDto<Raven.Server.Documents.Queries.Dynamic.DynamicQueryToIndexMatcher.Explanation> {
    IndexName: string;
}

interface virtualBulkOperationItem {
    id: string;
    date: string;
    duration: number;
    totalItemsProcessed: number;
    documentsProcessed: number;
    attachmentsProcessed: number;
    countersProcessed: number;
    timeSeriesProcessed: number;
}

interface virtualBulkOperationFailureItem {
    id: string;
    date: string;
    duration: number;
    errorMsg: string;
    error: string;
}

interface queryBasedVirtualBulkOperationItem extends virtualBulkOperationItem {
    query: string;
    indexOrCollectionUsed: string;
}

interface queryBasedVirtualBulkOperationFailureItem extends virtualBulkOperationItem {
    query: string;
    errorMsg: string;
    error: string;
}

type adminLogsHeaderType = "Source" | "Logger";

interface adminLogsConfiguration extends Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters {
    Path: string;
    CurrentMode: Sparrow.Logging.LogMode;
}

interface testEtlScriptResult {
    DebugOutput: Array<string>;
    TransformationErrors: Array<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>;
}

declare module Raven.Server.Documents.ETL.Providers.RelationalDatabase.Common.Test {
    interface RelationalDatabaseEtlTestScriptResult extends testEtlScriptResult {
    }
}

declare module Raven.Server.Documents.ETL.Providers.Raven.Test {
    interface RavenEtlTestScriptResult extends testEtlScriptResult {
    }
}

declare module Raven.Server.Documents.ETL.Providers.OLAP.Test {
    interface OlapEtlTestScriptResult extends testEtlScriptResult {
    }
}

declare module Raven.Server.Documents.ETL.Providers.ElasticSearch.Test {
    interface ElasticSearchEtlTestScriptResult extends testEtlScriptResult {
    }
}

declare module Raven.Server.Documents.ETL.Providers.Queue.Test {
    interface QueueEtlTestScriptResult extends testEtlScriptResult {
    }
}

type backupOptions = "None" | "Local" | "Azure" | "AmazonGlacier" | "AmazonS3" | "FTP" | "GoogleCloud";

interface periodicBackupServerLimitsResponse {
    LocalRootPath: string;
    AllowedAwsRegions: Array<string>;
    AllowedDestinations: Array<backupOptions>;
    EnableUnencryptedBackupForEncryptedDatabase: boolean;
}

interface serializedColumnDto {
    visible: boolean;
    editable: boolean;
    column: virtualColumnDto;
}

type valueProvider<T> = (arg: T) => any;
type sortMode = "asc" | "desc";

interface textColumnOpts<T> {
    extraClass?: (item: T) => string;
    useRawValue?: (item: T) => boolean;
    title?: (item:T) => string;
    headerTitle?: string;
    sortable?: "number" | "string" | valueProvider<T>;
    defaultSortOrder?: sortMode;
    customComparator?: (a: any, b: any) => number;
    transformValue?: ((a: any, item: T) => any) | ((a: any) => any);
}

interface hypertextColumnOpts<T> extends textColumnOpts<T> {
    handler?: (item: T, event: JQuery.TriggeredEvent) => void;
    extraClassForLink?: (item: T) => string;
    openInNewTab?: (item: T) => boolean;
}

interface multiNodeTagsColumnOpts<T> extends textColumnOpts<T> {
    nodeHrefAccessor?: (item: T, nodeTag: string) => string;
    nodeLinkTitleAccessor?: (item: T, nodeTag: string) => string;
}

type timeSeriesColumnEventType = "plot" | "preview";

interface timeSeriesColumnOpts<T> extends textColumnOpts<T> {
    handler?: (type: timeSeriesColumnEventType, documentId: string, name: string, value: timeSeriesQueryResultDto, event: JQuery.TriggeredEvent) => void;
}

interface virtualColumnDto {
    type: "flags" | "checkbox" | "text" | "hyperlink" | "custom" | "timeSeries" | "nodeTag" | "multiNodeTags" | "iconsPlusText";
    width: string;
    header: string;
    serializedValue: string;
}

interface editDocumentCrudActions {
    countersCount: KnockoutComputed<number>;
    setCounter(counter: counterItem): void;
    fetchCounters(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<counterItem>>;
    deleteCounter(counter: counterItem): void;

    attachmentsCount: KnockoutComputed<number>;
    fetchAttachments(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>>;
    deleteAttachment(file: attachmentItem): void;
    
    timeSeriesCount: KnockoutComputed<number>;
    fetchTimeSeries(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<timeSeriesItem>>;
    
    revisionsCount: KnockoutObservable<number>;
    fetchRevisionsCount(docId: string, db: database): void;
    
    saveRelatedItems(targetDocumentId: string): JQueryPromise<void>;
    onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any, forcedRevisionCreation: boolean): void;
}

interface confirmationDialogOptions {
    buttons?: string[];
    forceRejectWithResolve?: boolean;
    defaultOption?: string;
    html?: boolean;
    wideDialog?: boolean;
}

interface getIndexEntriesFieldsCommandResult {
    Static: string[];
    Dynamic: string[];
}

interface scrollColorConfig {
    trackColor: string;
    scrollColor: string;
}

type etlScriptDefinitionCacheItem = {
    etlType: EtlType;
    task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl |
                        Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl |
                        Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl |
                        Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl |
                        Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl |
                        Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl>;
}

type sinkScriptDefinitionCacheItem = {
    task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink>;
}

type addressType = "ipv4" | "ipv6" | "hostname" | "invalid";
type timeMagnitude = "minutes" | "hours" | "days";

interface pullReplicationExportFileFormat {
    Database: string;
    HubName: string;
    TopologyUrls: Array<string>;
    AccessName: string;
    Certificate: string;
    AllowHubToSinkMode: boolean;
    AllowSinkToHubMode: boolean;
    HubToSinkPrefixes: Array<string>;
    SinkToHubPrefixes: Array<string>;
    UseSamePrefixes: boolean;
}

interface certificateInfo {
    thumbprint: string;
    expiration: Date;
    notBefore: Date;
}

interface clusterWideStackTraceResponseItem {
    NodeTag: string;
    Stacks: Array<rawStackTraceResponseItem>;
    Threads: Array<Raven.Server.Dashboard.ThreadInfo>;
    NodeUrl: string;
    Error: string;
}

interface stackTracesResponseDto {
    Results: Array<rawStackTraceResponseItem>;
    Threads: Array<Raven.Server.Dashboard.ThreadInfo>;
}

interface rawStackTraceResponseItem {
    ThreadIds: number[],
    NativeThreads: boolean,
    StackTrace: string[];
}

interface threadStackTraceResponseDto {
    Results: Array<threadStackTraceResponseItem>;
    Threads: Array<Raven.Server.Dashboard.ThreadInfo>;
}


interface threadStackTraceResponseItem {
    OSThreadId: number;
    ManagedThreadId: number;
    IsNative: boolean;
    ThreadType: string;
    StackTrace: string[];
}

interface MigratorPathConfiguration {
    HasMigratorPath: boolean;
}

type timeSeriesResultType = "grouped" | "raw";

interface timeSeriesQueryResultDto {
    Count: number;
    "@metadata": {
        "@timeseries-named-values"?: string[];
    }
    Results: Array<timeSeriesQueryGroupedItemResultDto | timeSeriesRawItemResultDto>;
}

interface timeSeriesQueryGroupedItemResultDto {
    From: string;
    To: string;
    [column: string]: number[];
}

interface timeSeriesRawItemResultDto {
    Tag: string;
    Timestamp: string;
    Values: number[];
}

type timeUnit = "year" | "month" | "day" | "hour" | "minute" | "second";

type settingsTemplateType = Raven.Server.Config.ConfigurationEntryType | "StringArray" | "EnumArray" | "ServerWide";

interface TimeSeriesOperation extends Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation {
    Appends: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation[];
    Deletes: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.DeleteOperation[];
    Increments: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.IncrementOperation[];
}

type StudioTaskType = "Replication" | "PullReplicationAsHub" | "PullReplicationAsSink" | "Backup" | "Subscription" |
    "RavenEtl" | "SqlEtl" | "SnowflakeEtl" | "OlapEtl" | "ElasticSearchEtl" | "KafkaQueueEtl" | "RabbitQueueEtl" | "AzureQueueStorageQueueEtl" | "KafkaQueueSink" | "RabbitQueueSink";
    
type StudioEtlType = "Raven" | "Sql" | "Snowflake" | "Olap" | "ElasticSearch" | "Kafka" | "RabbitMQ" | "AzureQueueStorage";

type StudioQueueSinkType = "KafkaQueueSink" | "RabbitQueueSink";

type TaskDestinationType = "Collection" | "Table" | "Queue" | "Topic" | "Index";

interface sampleCode {
    title: string;
    text: string;
    html: string;
}

type OnlyStrings<T> = { [ P in keyof T]: T[P] extends string ? P : never }[keyof T & string];

type commandLineType = "PowerShell" | "Cmd" | "Bash";

interface geoPointInfo extends Raven.Server.Documents.Indexes.Spatial.Coordinates {
    PopupContent: document;
}

interface indexHistoryCommandResult {
    Index: string;
    History: Raven.Client.ServerWide.IndexHistoryEntry[];
}

interface databaseAccessInfo {
    dbName: string;
    accessLevel: databaseAccessLevel;
}

interface databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    hideDatabaseName: boolean;
    even: boolean;
    noData: boolean;
}

interface cachedDateValue<T> {
    date: Date;
    value: T;
}

type widgetType = Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType | "Welcome" | "License";

interface ioStatsWidgetConfig {
    splitIops?: boolean;
    splitThroughput?: boolean;
}

type databaseAccessLevel = `Database${Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess}`;
type securityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type accessLevel = databaseAccessLevel | securityClearance;

interface iconPlusText {
    iconClass: string;
    text: string;
    textClass?: string;
    title?: string;
}

interface ConfigureMicrosoftLogsDialogResult {
    isEnabled: boolean;
    configuration: object;
    persist: boolean;
}

interface columnPreviewFeature {
    install($tooltip: JQuery, valueProvider: () => any, elementProvider: () => any, containerSelector: string): void;
    syntax(column: virtualColumn, escapedValue: any, element: any): void;
}

interface indexErrorsCount {
    Name: string;
    Errors: IndexingErrorCount[];
}

interface IndexingErrorCount {
    Action: string;
    NumberOfErrors: number;
}

type databaseLocationSpecifier = {
    shardNumber?: number;
    nodeTag: string;
}


interface PopoverUtilsOptions extends PopoverOptions {
    rounded?: boolean;
}

interface StudioDatabasesResponse {
    Databases: StudioDatabaseInfo[];
    Orchestrators: StudioOrchestratorState[];
}

interface ReactDirtyFlag {
    setIsDirty: (isDirty: boolean, customDialog?: () => JQueryPromise<confirmDialogResult>) => void;
}

interface ReactInKnockoutOptions<T> {
    component: T;
    props?: Parameters<typeof T>[0];
    dirtyFlag?: ReactDirtyFlag;
}

type ReactInKnockout<T> = KnockoutComputed<ReactInKnockoutOptions<T>>;

interface Progress {
    processed: number;
    total: number;
}

interface rawTaskItem {
    type: StudioTaskType;
    dbName: string;
    count: number;
    node: string;
}

interface databaseDisconnectedEventArgs {
    databaseName: string;
    cause: databaseDisconnectionCause;
}

interface taskInfo {
    nameForUI: string;
    icon: string;
    colorClass: string;
}

type TombstoneItem = Raven.Server.Documents.TombstoneCleaner.StateHolder & { Collection: string };
type TombstonesStateOnWire = Omit<Raven.Server.Documents.TombstoneCleaner.TombstonesState, "Tombstones"> & { Results: TombstoneItem[] };

// Server ToJson() method converts the version object to a string
type LicenseStatus = Omit<Raven.Server.Commercial.LicenseStatus, "Version"> & { Version: string };


type SqlConnectionStringFactoryName =
    | "Microsoft.Data.SqlClient"
    | "System.Data.SqlClient"
    | "MySql.Data.MySqlClient"
    | "MySqlConnector.MySqlConnectorFactory"
    | "Npgsql"
    | "Oracle.ManagedDataAccess.Client";

type SqlConnectionString = Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString & { FactoryName: SqlConnectionStringFactoryName }
type GetConnectionStringsResult = Omit<Raven.Client.Documents.Operations.ConnectionStrings.GetConnectionStringsResult, "SqlConnectionStrings"> & {
    SqlConnectionStrings: {[key: string]: SqlConnectionString;};
}


type AzureQueueStorageAuthenticationType = "connectionString" | "entraId" | "passwordless";
