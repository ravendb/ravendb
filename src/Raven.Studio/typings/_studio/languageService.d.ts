/// <reference path="globals/ace/index.d.ts" />

interface BaseLanguageServiceRequest {
    id: number;
    msgType: "request";
}

interface LanguageServiceSyntaxRequest extends BaseLanguageServiceRequest {
    type: "syntax";
    query: string;
}

interface LanguageServiceAutoCompleteRequest extends BaseLanguageServiceRequest {
    type: "complete";
    query: string;
    position: { row: number; column: number };
}

interface LanguageServiceMetadataRequest extends BaseLanguageServiceRequest {
    type: "metadata";
    payload: MetadataRequestPayload;
}

type MetadataRequestListCollections = {
    type: "collections";
}

type MetadataRequestListIndexes = {
    type: "indexes";
}

type MetadataRequestPayload = MetadataRequestListCollections | MetadataRequestListIndexes;

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

type MetadataResponsePayload = MetadataResponseCollections | MetadataResponseIndexes;

type LanguageServiceResponse = LanguageServiceSyntaxResponse | LanguageServiceAutoCompleteResponse | LanguageServiceMetadataResponse;
