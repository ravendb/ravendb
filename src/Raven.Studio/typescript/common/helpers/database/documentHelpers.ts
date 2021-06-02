/// <reference path="../../../../typings/tsd.d.ts" />

import document = require("models/database/documents/document");

class documentHelpers {
    static findRelatedDocumentsCandidates(doc: documentBase): string[] {
        const results: string[] = [];
        const initialDocumentFields = doc.getDocumentPropertyNames();
        const documentNodesFlattenedList: any[] = [];

        // get initial nodes list to work with
        initialDocumentFields.forEach(curField => {
            documentNodesFlattenedList.push(doc[curField]);
        });

        for (let documentNodesCursor = 0; documentNodesCursor < documentNodesFlattenedList.length; documentNodesCursor++) {
            const curField = documentNodesFlattenedList[documentNodesCursor];
            if (typeof curField === "string" && curField.length < 512 && /\w+\/\w+/ig.test(curField)) {

                if (!results.find(x => x === curField.toString())) {
                    results.push(curField.toString());
                }
            } else if (typeof curField == "object" && !!curField) {
                for (let curInnerField in curField) {
                    var item = curField[curInnerField];
                    documentNodesFlattenedList.push(item);
                }
            }
        }
        return results;
    }

    static unescapeNewlinesAndTabsInTextFields(str: string): string {
        const AceDocumentClass = ace.require("ace/document").Document;
        const AceEditSessionClass = ace.require("ace/edit_session").EditSession;

        const AceJSONMode = (ace.require("ace/mode/json") || 
                           ace.require("ace/mode/raven_document") ||
                           ace.require("ace/mode/json_newline_friendly")).Mode;

        const documentTextAceDocument = new AceDocumentClass(str);
        const jsonMode = new AceJSONMode();
        const documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        try {
            const TokenIterator = ace.require("ace/token_iterator").TokenIterator;
            const iterator = new TokenIterator(documentTextAceEditSession, 0, 0);
            let curToken = iterator.getCurrentToken();
            // first, calculate newline indexes
            const rowsIndexes = str.split("").map((x, index) => ({
                char: x,
                index: index
            })).filter(x => x.char === "\n").map(x => x.index);

            // start iteration from the end of the document
            while (curToken) {
                curToken = iterator.stepForward();
            }
            curToken = iterator.stepBackward();

            let lastTextSectionPosEnd: { row: number, column: number } = null;

            while (curToken) {
                if (curToken.type === "string" || curToken.type == "constant.language.escape") {
                    if (lastTextSectionPosEnd == null) {
                        curToken = iterator.stepForward();
                        lastTextSectionPosEnd = {
                            row: iterator.getCurrentTokenRow(),
                            column: iterator.getCurrentTokenColumn() + 1
                        };
                        curToken = iterator.stepBackward();
                    }
                } else {
                    if (lastTextSectionPosEnd != null) {
                        curToken = iterator.stepForward();
                        const lastTextSectionPosStart = {
                            row: iterator.getCurrentTokenRow(),
                            column: iterator.getCurrentTokenColumn() + 1
                        };
                        const stringTokenStartIndexInSourceText = (lastTextSectionPosStart.row > 0 ? rowsIndexes[lastTextSectionPosStart.row - 1] : 0) + lastTextSectionPosStart.column;
                        const stringTokenEndIndexInSourceText = (lastTextSectionPosEnd.row > 0 ? rowsIndexes[lastTextSectionPosEnd.row - 1] : 0) + lastTextSectionPosEnd.column;
                        const newTextPrefix: string = str.substring(0, stringTokenStartIndexInSourceText);
                        const newTextSuffix: string = str.substring(stringTokenEndIndexInSourceText, str.length);
                        const newStringTokenValue: string = str.substring(stringTokenStartIndexInSourceText, stringTokenEndIndexInSourceText)
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
        } finally {
            documentTextAceEditSession.destroy();
        }
    }

    static escapeNewlinesAndTabsInTextFields(str: string): any {
        const AceDocumentClass = ace.require("ace/document").Document;
        const AceEditSessionClass = ace.require("ace/edit_session").EditSession;
        const AceJSONMode = (ace.require("ace/mode/json_newline_friendly") || ace.require("ace/mode/raven_document_newline_friendly")).Mode;
        const documentTextAceDocument = new AceDocumentClass(str);
        const jsonMode = new AceJSONMode();
        const documentTextAceEditSession = new AceEditSessionClass(documentTextAceDocument, jsonMode);
        try {
            let previousLine = 0;

            const tokenIterator = ace.require("ace/token_iterator").TokenIterator;
            const iterator = new tokenIterator(documentTextAceEditSession, 0, 0);
            let curToken = iterator.getCurrentToken();
            let text = "";
            while (curToken) {
                if (iterator.$row - previousLine > 1) {
                    const rowsGap = iterator.$row - previousLine;
                    for (let i = 0; i < rowsGap - 1; i++) {
                        text += "\\r\\n";
                    }
                }
                if (curToken.type === "string" || curToken.type === "constant.language.escape") {
                    if (previousLine < iterator.$row) {
                        text += "\\r\\n";
                    }

                    const newTokenValue = curToken.value
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
        } finally {
            documentTextAceEditSession.destroy();
        }
    }

    static findSchema(documents: Array<document>): document {
        try {
            documents.forEach(doc => {
                JSON.stringify(doc);
            });
        } catch (e) {
            throw new Error("Cannot find schema for not serializable objects");
        }

        const docDto = documentHelpers.findSchemaForObject(documents.map(x => x.toDto(false)));

        const metadatas = documents.map(x => x.__metadata);
        const collection = documentHelpers.findCommonValue(metadatas, "collection");
        const ravenClrType = documentHelpers.findCommonValue(metadatas, "ravenClrType");

        docDto["@metadata"] = {
            "@collection": collection,
            "Raven-Clr-Type": ravenClrType
        }
        return new document(docDto);
    }

    private static findSchemaForObject(objects: Array<any>): any {
        let result: any = {};

        let [firstDocument] = objects;

        const isArray = firstDocument instanceof Array;
        result = isArray ? [] : {};

        for (let prop in firstDocument) {
            if (firstDocument.hasOwnProperty(prop) === false)
                continue;

            const defaultValue = documentHelpers.findSchemaDefaultValue(objects, prop);
            if (typeof (defaultValue) === "undefined")
                continue;

            if (isArray) {
                result.push(defaultValue);
            } else {
                result[prop] = defaultValue;
            }
        }

        return result;
    }

    private static findCommonValue<T>(elements: Array<T>, property: keyof T): any {
        const extractedValues = elements.map(doc => doc[property]);
        const [firstValue, ...restValues] = extractedValues;

        for (let i = 0; i < restValues.length; i++) {
            if (firstValue !== restValues[i]) {
                return undefined;
            }
        }
        return firstValue;
    }

    private static findSchemaDefaultValue(documents: Array<any>, property: string): any {
        documents = documents.filter(x => x);
        for (let i = 0; i < documents.length; i++) {
            if (!(property in documents[i])) {
                return undefined;
            }
        }

        const extractedValues = documents.map(doc => doc[property]);
        const extractedTypes = extractedValues.map(v => typeof (v));
        let [firstType, ...restTypes] = extractedTypes;
        for (let i = 0; i < restTypes.length; i++) {
            if (firstType !== restTypes[i]) {
                firstType = "undefined";
                break;
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
