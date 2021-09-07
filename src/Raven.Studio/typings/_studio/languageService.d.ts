/// <reference path="globals/ace/index.d.ts" />

interface BaseLanguageServiceRequest {
    id: number;
}

interface LanguageServiceSyntaxRequest extends BaseLanguageServiceRequest {
    type: "syntax";
    data: string;
}

interface LanguageServiceAutoCompleteRequest extends BaseLanguageServiceRequest {
    type: "complete";
    data: string;
    position: { row: number; column: number };
}

type LanguageServiceRequest = LanguageServiceSyntaxRequest | LanguageServiceAutoCompleteRequest;

interface BaseLanguageServiceResponse {
    id: number;
}

interface LanguageServiceSyntaxResponse extends BaseLanguageServiceResponse {
    annotations: AceAjax.Annotation[];
}

interface LanguageServiceAutoCompleteResponse extends BaseLanguageServiceResponse {
    wordList: autoCompleteWordList[];
}

type LanguageServiceResponse = LanguageServiceSyntaxResponse | LanguageServiceAutoCompleteResponse;
