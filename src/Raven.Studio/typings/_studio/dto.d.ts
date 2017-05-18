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

interface IndexErrorPerDocument {
    Document: string;
    Error: string;
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
    '@etag': number;
    '@flags'?: string;
    '@attachments'?: Array<documentAttachmentDto>;
    '@change-vector'?: Array<changeVectorDto>;

}

interface updateDatabaseConfigurationsResult {
    ETag: number;
}
interface changeVectorDto {
    DbId: string;
    Etag: number;
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

type menuItemType = "separator" | "intermediate" | "leaf" | "collections";

interface menuItem {
    type: menuItemType;
    parent: KnockoutObservable<menuItem>;
}

type dynamicHashType = KnockoutObservable<string> | (() => string);

interface singleAuthToken {
    Token: string;
}

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

interface transformerParamInfo {
    name: string;
    hasDefault: boolean;
}
interface operationIdDto {
    OperationId: number;
}

interface databaseCreatedEventArgs {
    qualifier: string;
    name: string;
}

interface availableConfigurationSection {
    name: string;
    alwaysEnabled: boolean;
    enabled: KnockoutObservable<boolean>;
    validationGroup?: KnockoutValidationGroup;
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

interface transformerQueryDto {
    transformerName: string;
    queryParams: Array<transformerParamDto>;
}

interface transformerParamDto {
    name: string;
    value: string;
}

interface queryDto {
    indexName: string;
    queryText: string;
    sorts: string[];
    transformerQuery: transformerQueryDto;
    showFields: boolean;
    indexEntries: boolean;
    useAndOperator: boolean;
}

interface storedQueryDto extends queryDto {
    hash: number;
}

interface replicationConflictListItemDto {
    Id: string;
    LastModified: string;
}

type databaseDisconnectionCause = "Error" | "DatabaseDeleted" | "DatabaseDisabled" | "ChangingDatabase";

type querySortType = "Ascending" | "Descending" | "Range Ascending" | "Range Descending";

interface recentErrorDto extends Raven.Server.NotificationCenter.Notifications.Notification {
    Details: string;
    HttpStatus?: string;
}

declare module studio.settings {
    type numberFormatting = "raw" | "formatted";
    type dontShowAgain = "EditSystemDocument";
    type saveLocation = "local" | "remote";
    type usageEnvironment = "Default" | "Dev" | "Test" | "Prod";
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

interface subscriptionResponseItemDto {
    SubscriptionId: number;
    Criteria: Raven.Client.Documents.Subscriptions.SubscriptionCriteria;
    AckEtag: number;
    TimeOfReceivingLastAck: string;
    Connection: subscriptionConnectionInfoDto;
    RecentConnections: Array<subscriptionConnectionInfoDto>;
    RecentRejectedConnections: Array<subscriptionConnectionInfoDto>;
}

interface subscriptionConnectionInfoDto {
    ClientUri: string;
    ConnectionException: string;
    Stats: Raven.Server.Documents.Subscriptions.SubscriptionConnectionStats;
    Options: Raven.Client.Documents.Subscriptions.SubscriptionConnectionOptions;
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

interface pagedResultWithAvailableColumns<T> extends pagedResult<T> {
    availableColumns: string[];
}

interface clusterTopologyDto {
    Topology: Raven.Client.Http.ClusterTopology;
    Leader: string;
    NodeTag: string;
}

type clusterNodeType = "Member" | "Promotable" | "Watcher";

type patchOption = "Document" | "Collection" | "Index";

interface patchDto extends documentDto {
    PatchOnOption: patchOption;
    Query: string;
    Script: string;
    SelectedItem: string;
}

interface feedbackSavedSettingsDto {
    Name: string;
    Email: string;
}

interface customFunctionsDto {
    Functions: string;
}