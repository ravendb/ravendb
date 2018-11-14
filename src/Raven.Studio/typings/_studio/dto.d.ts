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
    '@expires'?: string;
}

interface updateDatabaseConfigurationsResult {
    RaftCommandIndex: number;
}

interface nodeCounterValue {
    nodeTag: string;
    nodeFullId: string;
    nodeShortId: string;
    nodeCounterValue: number;
}

interface counterItem {
    documentId: string;
    counterName: string;
    totalCounterValue: number;
    counterValuesPerNode: Array<nodeCounterValue>;
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
    type: menuItemType;
    parent: KnockoutObservable<menuItem>;
}

type dynamicHashType = KnockoutObservable<string> | (() => string);

interface chagesApiConfigureRequestDto {
    Command: string;
    Param?: string;
}

interface changedOnlyMetadataFieldsDto extends documentMetadataDto {
    Method: string;
}

interface saveDocumentResponseDto {
    Results: Array<changedOnlyMetadataFieldsDto>;
}

interface operationIdDto {
    OperationId: number;
}

interface databaseCreatedEventArgs {
    qualifier: string;
    name: string;
}

type availableConfigurationSectionId =  "restore" | "legacyMigration" | "encryption" | "replication" | "path";

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

interface detailedStorageReportItemDto {
    Name: string;
    Type: string;
    Report: Voron.Debugging.DetailedStorageReport;
}

interface arrayOfResultsAndCountDto<T> {
    Results: T[];
    Count: number;
}

interface timeGapInfo {
    durationInMillis: number;
    start: Date;
}
interface documentColorPair {
    docName: string;
    docColor: string;
}

interface aggregatedRange {
    start: number;
    end: number;
    value: number;
}

interface indexesWorkData {
    pointInTime: number;
    numberOfIndexesWorking: number;
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

interface IOMetricsRecentStatsWithCache extends Raven.Server.Documents.Handlers.IOMetricsRecentStats {
    StartedAsDate: Date; // used for caching
    CompletedAsDate: Date; // used for caching
}

interface ReplicationPerformanceBaseWithCache extends Raven.Client.Documents.Replication.ReplicationPerformanceBase {
    StartedAsDate: Date;
    CompletedAsDate: Date;
    Type: Raven.Server.Documents.Replication.LiveReplicationPerformanceCollector.ReplicationPerformanceType;
    Description: string;
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

type virtualNotificationType = "CumulativeBulkInsert" | "AttachmentUpload";

declare module Raven.Server.NotificationCenter.Notifications {
    interface Notification  {
        // extend server side type to contain local virtual notifications 
        Type: Raven.Server.NotificationCenter.Notifications.NotificationType | virtualNotificationType;
    }
}

interface explainQueryResponse extends resultsDto<Raven.Server.Documents.Queries.Dynamic.DynamicQueryToIndexMatcher.Explanation> {
    IndexName: string;
}


interface virtualBulkInsertItem {
    id: string;
    date: string;
    duration: number;
    items: number;
}

type adminLogsHeaderType = "Source" | "Logger";

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

type backupOptions = "None" | "Local" | "Azure" | "AmazonGlacier" | "AmazonS3" | "FTP";

interface periodicBackupServerLimitsResponse {
    LocalRootPath: string;
    AllowedAwsRegions: Array<string>;
    AllowedDestinations: Array<backupOptions>;
}

interface serializedColumnDto {
    visible: boolean;
    editable: boolean;
    column: virtualColumnDto;
}


interface virtualColumnDto {
    type: "flags" | "checkbox" | "text" | "hyperlink" | "custom"
    width: string;
    header: string;
    serializedValue: string;
}

interface attachmentItem {
    documentId: string;
    name: string;
    contentType: string;
    size: number;
}

interface editDocumentCrudActions {
    setCounter(counter: counterItem): void;
    deleteAttachment(file: attachmentItem): void;
    deleteCounter(counter: counterItem): void;
    fetchCounters(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<counterItem>>;
    fetchAttachments(nameFilter: string, skip: number, take: number): JQueryPromise<pagedResult<attachmentItem>>;
    saveRelatedItems(targetDocumentId: string): JQueryPromise<void>;
    attachmentsCount: KnockoutComputed<number>;
    countersCount: KnockoutComputed<number>;
    onDocumentSaved(saveResult: saveDocumentResponseDto, localDoc: any): void;
}



interface getIndexEntriesFieldsCommandResult {
    Static: string[];
    Dynamic: string[];
}
