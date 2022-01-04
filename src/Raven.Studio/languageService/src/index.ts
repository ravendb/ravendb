/// <reference path="../../typings/tsd.d.ts"/>

import { parseRql } from "./parser";
import { CaretPosition } from "./types";
import { autoCompleteEngine } from "./autocomplete";
import { proxyMetadataProvider } from "../../typescript/common/autoComplete/proxyMetadataProvider";

let nextRequestId = 1;

export function sendRequest(msg: LanguageServiceRequest) {
    postMessage(msg, undefined);
}

export function sendResponse(msg: LanguageServiceResponse) {
    postMessage(msg, undefined);
}

const metadataProvider = new proxyMetadataProvider(payload => {
    const requestId = nextRequestId++;

    sendRequest({
        id: requestId,
        msgType: "request",
        type: "metadata",
        payload
    });

    return requestId;
});

const engine = new autoCompleteEngine(metadataProvider);

export function handleSyntaxCheck(input: string, queryType: rqlQueryType): AceAjax.Annotation[] {
    const response: AceAjax.Annotation[] = [];
    
    parseRql(input, {
        onParserError: (recognizer, offendingSymbol, line, charPositionInLine, msg) => {
            response.push({
                type: "warning",
                row: line - 1,
                text: msg,
                column: charPositionInLine
            });
        }
    });

    return response;
}

onmessage = async (e: MessageEvent<LanguageServiceRequest | LanguageServiceResponse>): Promise<void> => {
    if (e.data.msgType === "request") {
        switch (e.data.type) {
            case "syntax":
                const annotations = handleSyntaxCheck(e.data.query, e.data.queryType);

                sendResponse({
                    id: e.data.id,
                    annotations,
                    msgType: "response"
                });
                break;
            case "complete":
                const position = e.data.position;
                const caret: CaretPosition = {
                    line: position.row + 1,
                    column: position.column
                };
                const query = e.data.query;
                const queryType = e.data.queryType;
                const wordList = await engine.complete(query, caret, queryType);

                sendResponse({
                    msgType: "response",
                    id: e.data.id,
                    wordList
                });
                break;
            default:
                throw new Error("Unhandled message type: " + e.data);
        }
    }
    
    if (e.data.msgType === "response") {
        metadataProvider.onResponse(e.data.id, (e.data as LanguageServiceMetadataResponse).response);
    }
}
