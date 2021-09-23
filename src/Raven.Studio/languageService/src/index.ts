/// <reference path="../../typings/tsd.d.ts"/>

import { parseRql } from "./parser";
import { handleAutoComplete } from "./autocomplete";
import { CaretPosition } from "./types";

export function sendResponse(response: LanguageServiceResponse) {
    postMessage(response, undefined);
}

export function handleSyntaxCheck(input: string): AceAjax.Annotation[] {
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
    
    //TODO: we might check for duplicates / invalid index name etc in future

    return response;
}

onmessage = async (e: MessageEvent<LanguageServiceRequest>): Promise<void> => {
    switch (e.data.type) {
        case "syntax":
            const annotations = handleSyntaxCheck(e.data.query);
            
            sendResponse({
                id: e.data.id,
                annotations
            });
            break;
        case "complete":
            const position = e.data.position;
            const caret: CaretPosition = {
                line: position.row + 1,
                column: position.column
            };
            const query = e.data.query;
            const wordList = await handleAutoComplete(query, caret);
            
            sendResponse({
                id: e.data.id,
                wordList
            });
            break;
        default:
            throw new Error("Unhandled message type: " + e.data);
    }
}
