/// <reference path="../tsd.d.ts"/>

interface queryResultDto {
    Results: any[];
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

interface importDatabaseRequestDto {
    batchSize: number,
}