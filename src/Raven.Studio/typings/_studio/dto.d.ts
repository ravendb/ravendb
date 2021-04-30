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

interface debugGraphOutputNode {
    Id: string;
    Value: documentDto;
}

interface debugGraphEdge {
    Name: string;
    Results: Array<{
        From: string;
        To: string;
        Edge: any;
    }>
}

interface debugGraphOutputResponse {
    Edges: Array<debugGraphEdge>,
    Nodes: Array<debugGraphOutputNode>;
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

interface resultsWithCountAndAvailableColumns<T> extends resultsWithTotalCountDto<T> {
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
    '@counters'?: Array<string>;
    '@counters-snapshot'?: dictionary<number>;
    '@timeseries-snapshot'?: dictionary<revisionTimeSeriesDto>;
    '@timeseries': Array<string>;
    '@expires'?: string;
    '@refresh'?: string;
}

interface updateDatabaseConfigurationsResult {
    RaftCommandIndex: number;
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

type dynamicHashType = KnockoutObservable<string> | (() => string);

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

interface operationIdDto {
    OperationId: number;
}

type availableConfigurationSectionId = "restore" | "legacyMigration" | "encryption" | "replication" | "path";

type restoreSource = "local" | "cloud" | "amazonS3" | "azure" | "googleCloud";

interface restoreTypeAware {
    Type: Raven.Client.Documents.Operations.Backups.RestoreType;
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

interface availableConfigurationSection {
    name: string;
    id: availableConfigurationSectionId;
    alwaysEnabled: boolean;
    disableToggle: KnockoutObservable<boolean>;
    enabled: KnockoutObservable<boolean>;
    validationGroup?: KnockoutValidationGroup;
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
}

type databaseDisconnectionCause = "Error" | "DatabaseDeleted" | "DatabaseDisabled" | "ChangingDatabase" | "DatabaseIsNotRelevant";

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
    StartedAsDate: Date; // used for caching
    CompletedAsDate: Date; // user for caching
}

interface IOMetricsRecentStatsWithCache extends Raven.Server.Utils.IoMetrics.IOMetricsRecentStats {
    StartedAsDate: Date; // used for caching
    CompletedAsDate: Date; // used for caching
}

type subscriptionType =  "SubscriptionConnection" | "SubscriptionBatch" | "AggregatedBatchesInfo";

type ongoingTaskStatType = Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType |
                           Raven.Client.Documents.Operations.ETL.EtlType |
                           subscriptionType;

interface ReplicationPerformanceBaseWithCache extends Raven.Client.Documents.Replication.ReplicationPerformanceBase {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType;
    Description: string;
    HasErrors: boolean;
}

interface EtlPerformanceBaseWithCache extends Raven.Server.Documents.ETL.Stats.EtlPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: Raven.Client.Documents.Operations.ETL.EtlType;
    HasErrors: boolean;
}

interface SubscriptionConnectionPerformanceStatsWithCache extends Raven.Server.Documents.Subscriptions.Stats.SubscriptionConnectionPerformanceStats {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: subscriptionType;
    HasErrors: boolean;
}

interface SubscriptionBatchPerformanceStatsWithCache extends Raven.Server.Documents.Subscriptions.SubscriptionBatchPerformanceStats {
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
    highlightings?: dictionary<dictionary<Array<string>>>;
    explanations?: dictionary<Array<string>>;
    timings?: Raven.Client.Documents.Queries.Timings.QueryTimings;
}

interface pagedResultWithAvailableColumns<T> extends pagedResult<T> {
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
    meta: string 
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
    terms: (indexName: string, collection: string, field: string, pageSize: number, callback: (terms: string[]) => void) => void;
    indexFields: (indexName: string, callback: (fields: string[]) => void) => void;
    collectionFields: (collectionName: string, prefix: string, callback: (fields: dictionary<string>) => void) => void;
    collections: (callback: (collectionNames: string[]) => void) => void;
    indexNames: (callback: (indexNames: string[]) => void) => void;
}

type rqlQueryType = "Select" | "Update";

type autoCompleteCompleter = (editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: autoCompleteWordList[]) => void) => void;
type certificateMode = "generate" | "upload" | "editExisting" | "replace";

type dbCreationMode = "newDatabase" | "restore" | "legacyMigration";

type legacySourceType = "ravendb" | "ravenfs";
type legacyEncryptionAlgorithms = "DES" | "RC2" | "Rijndael" | "Triple DES";


interface unifiedCertificateDefinition extends Raven.Client.ServerWide.Operations.Certificates.CertificateDefinition {
    Thumbprints: Array<string>;
    Visible: KnockoutObservable<boolean>;
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

type virtualNotificationType = "CumulativeBulkInsert" | "AttachmentUpload" | "CumulativeUpdateByQuery" | "CumulativeDeleteByQuery";

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

interface queryBasedVirtualBulkOperationItem extends virtualBulkOperationItem {
    query: string;
    indexOrCollectionUsed: string;
}

type adminLogsHeaderType = "Source" | "Logger";

interface adminLogsConfiguration extends Raven.Client.ServerWide.Operations.Logs.SetLogsConfigurationOperation.Parameters {
    Path: string;
    CurrentMode: Sparrow.Logging.LogMode;
}

declare module Raven.Server.Documents.ETL.Providers.SQL.Test {
    interface SqlEtlTestScriptResult {
        DebugOutput: Array<string>;
        TransformationErrors: Array<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>;
    }
}

declare module Raven.Server.Documents.ETL.Providers.Raven.Test {
    interface RavenEtlTestScriptResult extends Raven.Server.Documents.ETL.Test.TestEtlScriptResult {
        DebugOutput: Array<string>;
        TransformationErrors: Array<Raven.Server.NotificationCenter.Notifications.Details.EtlErrorInfo>;
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
    sortable?: "number" | "string" | valueProvider<T>;
    defaultSortOrder?: sortMode;
    customComparator?: (a: any, b: any) => number;
}

interface hypertextColumnOpts<T> extends textColumnOpts<T> {
    handler?: (item: T, event: JQueryEventObject) => void;
    extraClassForLink?: (item: T) => string;
    openInNewTab?: (item: T) => boolean;
}

type timeSeriesColumnEventType = "plot" | "preview";

interface timeSeriesColumnOpts<T> extends textColumnOpts<T> {
    handler?: (type: timeSeriesColumnEventType, documentId: string, name: string, value: timeSeriesQueryResultDto, event: JQueryEventObject) => void;
}

interface virtualColumnDto {
    type: "flags" | "checkbox" | "text" | "hyperlink" | "custom" | "timeSeries" | "nodeTag";
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
    etlType: Raven.Client.Documents.Operations.ETL.EtlType;
    task: JQueryPromise<Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlDetails | Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlDetails>;
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

type indexStatus = "Normal" | "ErrorOrFaulty" | "Stale" | "Paused" | "Disabled" | "Idle";

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

type settingsTemplateType = Raven.Server.Config.ConfigurationEntryType | "StringArray" | "EnumArray" | "ServeWide";

interface TimeSeriesOperation extends Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation {
    Appends: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.AppendOperation[];
    Deletes: Raven.Client.Documents.Operations.TimeSeries.TimeSeriesOperation.DeleteOperation[];
}

type TasksNamesInUI = "External Replication" | "RavenDB ETL" | "SQL ETL" | "Backup" | "Subscription" | "Replication Hub" | "Replication Sink";

interface sampleCode {
    title: string;
    text: string;
    html: string;
}

type OnlyStrings<T> = { [ P in keyof T]: T[P] extends string ? P : never }[keyof T & string];

interface geoPointInfo extends Raven.Server.Documents.Indexes.Spatial.Coordinates {
    PopupContent: document;
}

interface indexHistoryCommandResult {
    Index: string;
    History: Raven.Client.ServerWide.IndexHistoryEntry[];
}

interface databaseAccessInfo {
    dbName: string;
    accessLevel: Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess;
}


interface databaseAndNodeAwareStats {
    database: string;
    nodeTag: string;
    hideDatabaseName: boolean;
    even: boolean;
}

interface cachedDateValue<T> {
    date: Date;
    value: T;
}

type widgetType = Raven.Server.Dashboard.Cluster.ClusterDashboardNotificationType | "Welcome" | "License";

type databaseAccessLevel = `Database${Raven.Client.ServerWide.Operations.Certificates.DatabaseAccess}`;
type securityClearance = Raven.Client.ServerWide.Operations.Certificates.SecurityClearance;
type accessLevel = databaseAccessLevel | securityClearance;
