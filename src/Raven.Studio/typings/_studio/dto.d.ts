/// <reference path="../tsd.d.ts"/>

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

interface disableResourceResult {
    QualifiedName: string;
    Success: boolean;
    Reason: string;
    Disabled: boolean;
}

interface saveIndexResult {
    IndexId: number;
    Index: string;
}

interface deleteResourceConfirmResult extends confirmDialogResult {
    keepFiles: boolean;
}

type menuItemType = "separator" | "intermediate" | "leaf";

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

interface saveDocumentResponseDto {
    Results: Array<saveDocumentResponseItemDto>;
}

interface saveDocumentResponseItemDto {
    Key: string;
    Etag: number;
    Method: string;
    AdditionalData: any;
    Metadata?: documentMetadataDto; 
    PatchResult: string;
    Deleted: boolean;
}

interface transformerParamInfo {
    name: string;
    hasDefault: boolean;
}
interface operationIdDto {
    OperationId: number;
}

interface resourceCreatedEventArgs {
    qualifier: string;
    name: string;
}

interface availableBundle {
    displayName: string;
    name: string;
    hasAdvancedConfiguration: boolean;
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

type resourceDisconnectionCause = "Error" | "ResourceDeleted" | "ResourceDisabled" | "ChangingResource";

type querySortType = "Ascending" | "Descending" | "Range Ascending" | "Range Descending";

interface recentErrorDto extends Raven.Server.NotificationCenter.Notifications.Notification {
    Details: string;
    HttpStatus?: string;
}
