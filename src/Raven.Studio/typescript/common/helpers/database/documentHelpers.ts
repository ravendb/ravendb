/// <reference path="../../../../typings/tsd.d.ts" />

import document = require("models/database/documents/document");

class documentHelpers {
    static findRelatedDocumentsCandidates(doc: documentBase): string[] {
        var results: string[] = [];
        var initialDocumentFields = doc.getDocumentPropertyNames();
        var documentNodesFlattenedList: any[] = [];

        // get initial nodes list to work with
        initialDocumentFields.forEach(curField => {
            documentNodesFlattenedList.push(doc[curField]);
        });

        for (var documentNodesCursor = 0; documentNodesCursor < documentNodesFlattenedList.length; documentNodesCursor++) {
            var curField = documentNodesFlattenedList[documentNodesCursor];
            if (typeof curField === "string" && /\w+\/\w+/ig.test(curField)) {

                if (!results.first(x => x === curField.toString())) {
                    results.push(curField.toString());
                }
            }
            else if (typeof curField == "object" && !!curField) {
                for (var curInnerField in curField) {
                    documentNodesFlattenedList.push(curField[curInnerField]);
                }
            }
        }
        return results;
    }

    static unescapeNewlinesAndTabsInTextFields(str: string): string {
        var AceDocumentClass = require("ace/document").Document;
        var AceEditSessionClass = require("ace/edit_session").EditSession;
        var AceJSONMode = require("ace/mode/json").Mode;
        var documentTextAceDocument = new AceDocumentClass(str);
        var jsonMode = new AceJSONMode();
        var documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        var TokenIterator = require("ace/token_iterator").TokenIterator;
        var iterator = new TokenIterator(documentTextAceEditSession, 0, 0);
        var curToken = iterator.getCurrentToken();
        // first, calculate newline indexes
        var rowsIndexes = str.split("").map((x, index) => ({ char: x, index: index })).filter(x => x.char === "\n" ).map(x => x.index);

        // start iteration from the end of the document
        while (curToken) {
            curToken = iterator.stepForward();
        }
        curToken = iterator.stepBackward();

        var lastTextSectionPosEnd: { row: number, column: number } = null;

        while (curToken) {
            if (curToken.type === "string" || curToken.type == "constant.language.escape") {
                if (lastTextSectionPosEnd == null) {
                    curToken = iterator.stepForward();
                    lastTextSectionPosEnd = { row: iterator.getCurrentTokenRow(), column: iterator.getCurrentTokenColumn() + 1 };
                    curToken = iterator.stepBackward();
                }
            }
            else {
                if (lastTextSectionPosEnd != null) {
                    curToken = iterator.stepForward();
                    var lastTextSectionPosStart = { row: iterator.getCurrentTokenRow(), column: iterator.getCurrentTokenColumn() + 1 };
                    var stringTokenStartIndexInSourceText = (lastTextSectionPosStart.row > 0 ? rowsIndexes[lastTextSectionPosStart.row - 1] : 0) + lastTextSectionPosStart.column;
                    var stringTokenEndIndexInSourceText = (lastTextSectionPosEnd.row > 0 ? rowsIndexes[lastTextSectionPosEnd.row - 1] : 0) + lastTextSectionPosEnd.column;
                    var newTextPrefix: string = str.substring(0, stringTokenStartIndexInSourceText);
                    var newTextSuffix: string = str.substring(stringTokenEndIndexInSourceText, str.length);
                    var newStringTokenValue: string = str.substring(stringTokenStartIndexInSourceText, stringTokenEndIndexInSourceText)
                        .replace(/(\\\\n|\\\\r\\\\n|\\n|\\r\\n|\\t|\\\\t)/g, (x) => {
                            if (x == "\\\\n" || x === "\\\\r\\\\n") {
                                return "\\r\\n";
                            } else if (x === "\\n" || x === "\\r\\n") {
                                return "\r\n";
                            } else if (x === "\\t") {
                                return "\t";
                            } else if (x === "\\\\t") {
                                return "\\t";
                            } else {
                                return "\r\n";
                            }
                        });

                    str = newTextPrefix + newStringTokenValue + newTextSuffix;
                    curToken = iterator.stepBackward();
                }
                lastTextSectionPosEnd = null;
            }

            curToken = iterator.stepBackward();
        }

        return str;
    }

    static escapeNewlinesAndTabsInTextFields(str: string): any {
        var AceDocumentClass = require("ace/document").Document;
        var AceEditSessionClass = require("ace/edit_session").EditSession;
        var AceJSONMode = require("ace/mode/json_newline_friendly").Mode;
        var documentTextAceDocument = new AceDocumentClass(str);
        var jsonMode = new AceJSONMode();
        var documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        var previousLine = 0;

        var TokenIterator = require("ace/token_iterator").TokenIterator;
        var iterator = new TokenIterator(documentTextAceEditSession, 0, 0);
        var curToken = iterator.getCurrentToken();
        var text = "";
        while (curToken) {
            if (iterator.$row - previousLine > 1) {
                var rowsGap = iterator.$row - previousLine;
                for (var i = 0; i < rowsGap - 1; i++) {
                    text += "\\r\\n";
                }
            }
            if (curToken.type === "string" || curToken.type === "constant.language.escape") {
                if (previousLine < iterator.$row) {
                    text += "\\r\\n";
                }

                var newTokenValue = curToken.value
                    .replace(/(\r\n)/g, '\\r\\n')
                    .replace(/(\n)/g, '\\n')
                    .replace(/(\t)/g, '\\t');
                text += newTokenValue;
            } else {
                text += curToken.value;
            }

            previousLine = iterator.$row;
            curToken = iterator.stepForward();
        }

        return text;
    }

    static findSchema(documents: Array<any>): any {
        try {
            documents.forEach(doc => {
                JSON.stringify(doc);
            });
        } catch (e) {
            throw new Error("Cannot find schema for not serializable objects");
        }

        return documentHelpers.findSchemaForObject(documents);
    }

    private static findSchemaForObject(objects: Array<any>): any {
        let result: any = {};

        let [firstDocument] = objects;

        for (let prop in firstDocument) {
            if (firstDocument.hasOwnProperty(prop)) {
                var defaultValue = documentHelpers.findSchemaDefaultValue(objects, prop);
                if (typeof (defaultValue) !== "undefined") {
                    result[prop] = defaultValue;
                }
            }
        }
        return result;
    }

    private static findSchemaDefaultValue(documents: Array<any>, property: string): any {
        for (let i = 0; i < documents.length; i++) {
            if (!(property in documents[i])) {
                return undefined;
            }
        }

        const extractedValues = documents.map(doc => doc[property]);
        const extractedTypes = extractedValues.map(v => typeof (v));
        const [firstType, ...restTypes] = extractedTypes;
        for (let i = 0; i < restTypes.length; i++) {
            if (firstType !== restTypes[i]) {
                continue;
            }
        }

        switch (firstType) {
            case "undefined":
                return undefined;
            case "number":
                return 0;
            case "boolean":
                return false;
            case "string":
                return "";
            case "function":
                return undefined;
            case "object":
                return documentHelpers.findSchemaForObject(extractedValues);
            default:
                throw new Error("Unhandled type: " + firstType);
        }
    }
    
}

export = documentHelpers;
