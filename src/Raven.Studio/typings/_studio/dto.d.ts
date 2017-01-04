/// <reference path="../tsd.d.ts"/>

interface queryResultDto<T> {
    Results: T[];
    Includes: any[];
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
    qualifiedName: string;
    success: boolean;
    reason: string;
    disabled: boolean;
}

interface deleteResourceResult {
    name: string;
    deleted: boolean;
    reason: string;
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

interface collectionsStatsDto {
    NumberOfDocuments: number;
    Collections: dictionary<number>;
}

interface singleAuthToken {
    Token: string;
}

interface chagesApiConfigureRequestDto {
    Command: string;
    Param?: string;
}

interface localStorageOperationsDto {
    ServerStartTime: string;
    Operations: Array<number>;    
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

interface ioMetricsResponse {
    Environments: Array<ioMetricsEnvironment>
}

interface ioMetricsEnvironment {
    Path: string;
    Files: Array<ioMetricsFileStats>;
}

interface ioMetricsFileStats {
    File: string,
    Status: "Closed" | "InUse",
    Recent: Array<ioMetricsRecentStats>;
    History: Array<ioMetricsHistoryStats>;
}

interface ioMetricsRecentStats {
    Start: string;
    Size: number;
    HumaneSize: string; 
    Duration: number;
    Type: ioMetricsType;
}

interface ioMetricsHistoryStats {
    Start: string;
    End: string;
    Size: number;
    HumaneSize: string;
    Duration: number;
    ActiveDuration: number;
    MaxDuration: number;
    MinDuration: number;
    Type: ioMetricsType;
}

type ioMetricsType = "JournalWrite" | "DataFlush" | "DataSync";

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

type resourceDisconnectionCause = "Error" | "ResourceDeleted" | "ResourceDisabled" | "ChangingResource";

type alertType = "Info" | "Success" | "Error" | "Warning";
