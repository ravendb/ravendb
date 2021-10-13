/// <reference path="globals/ace/index.d.ts" />

interface BaseLanguageServiceRequest {
    id: number;
    msgType: "request";
}

interface LanguageServiceSyntaxRequest extends BaseLanguageServiceRequest {
    type: "syntax";
    queryType: rqlQueryType;
    query: string;
}

interface LanguageServiceAutoCompleteRequest extends BaseLanguageServiceRequest {
    type: "complete";
    query: string;
    queryType: rqlQueryType;
    position: { row: number; column: number };
}

interface LanguageServiceMetadataRequest extends BaseLanguageServiceRequest {
    type: "metadata";
    payload: MetadataRequestPayload;
}

type MetadataRequestListCollections = {
    type: "collections";
}

type MetadataRequestListCollectionFields = {
    type: "collectionFields",
    collectionName: string,
    prefix: string
}

type MetadataRequestListIndexFields = {
    type: "indexFields",
    indexName: string
}

type MetadataRequestListIndexes = {
    type: "indexes";
}

type MetadataRequestPayload = MetadataRequestListCollections 
    | MetadataRequestListIndexes 
    | MetadataRequestListCollectionFields
    | MetadataRequestListIndexFields;

type LanguageServiceRequest = LanguageServiceSyntaxRequest | LanguageServiceAutoCompleteRequest | LanguageServiceMetadataRequest;

interface BaseLanguageServiceResponse {
    id: number;
    msgType: "response"
}

interface LanguageServiceSyntaxResponse extends BaseLanguageServiceResponse {
    annotations: AceAjax.Annotation[];
}

interface LanguageServiceAutoCompleteResponse extends BaseLanguageServiceResponse {
    wordList: autoCompleteWordList[];
}

interface LanguageServiceMetadataResponse extends BaseLanguageServiceResponse {
    response: MetadataResponsePayload;
}

type MetadataResponseCollections = {
    names: string[];
}

type MetadataResponseIndexes = {
    names: string[];
}

type MetadataResponseCollectionFields = {
    fields: Record<string, string>;
}

type MetadataResponseIndexFields = {
    fields: string[];
}

type MetadataResponsePayload = MetadataResponseCollections 
    | MetadataResponseIndexes 
    | MetadataResponseIndexFields 
    | MetadataResponseCollectionFields;

type LanguageServiceResponse = LanguageServiceSyntaxResponse | LanguageServiceAutoCompleteResponse | LanguageServiceMetadataResponse;
