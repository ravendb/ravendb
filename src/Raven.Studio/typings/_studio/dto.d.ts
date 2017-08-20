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

}

interface updateDatabaseConfigurationsResult {
    RaftCommandIndex: number;
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

interface availableConfigurationSection {
    name: string;
    alwaysEnabled: boolean;
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
    queryText: string;
    showFields: boolean;
    indexEntries: boolean;
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
    CurrentTerm: number;
    NodeTag: string;
    Status: { [key: string]: Raven.Client.Http.NodeStatus; };
}

type clusterNodeType = "Member" | "Promotable" | "Watcher";
type databaseGroupNodeType = "Member" | "Promotable" | "Rehab";
type patchOption = "Document" | "Query";
type subscriptionStartType = 'Beginning of Time' | 'Latest Document' | 'Change Vector';

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

interface externalReplicationDataFromUI {
    TaskName: string;
    DestinationDB: string;
    DestinationURL: string;
} 

interface subscriptionDataFromUI {
    TaskName: string;
    Script: string;
    Collection: string;
    ChangeVectorEntry: string;
    IncludeRevisions: boolean;
} 


interface layoutable {
    x: number;
    y: number;
}


interface autoCompleteWordList {
    caption: string; 
    value: string; 
    score: number; 
    meta: string 
}

type autoCompleteCompleter = (editor: AceAjax.Editor, session: AceAjax.IEditSession, pos: AceAjax.Position, prefix: string, callback: (errors: any[], wordlist: autoCompleteWordList[]) => void) => void;
