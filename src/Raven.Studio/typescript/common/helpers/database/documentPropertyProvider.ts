/// <reference path="../../../../typings/tsd.d.ts" />

import document = require("models/database/documents/document");
import database = require("models/resources/database");
import getDocumentsPreviewCommand = require("commands/database/documents/getDocumentsPreviewCommand");
import getDocumentWithMetadataCommand = require("commands/database/documents/getDocumentWithMetadataCommand");
import messagePublisher = require("common/messagePublisher");

class documentPropertyProvider {
    private fullDocumentsCache = new Map<string, document>();

    private readonly db: database;

    constructor(db: database) {
        this.db = db;
    }

    resolvePropertyValue(doc: document, valueAccessor: ((item: document) => any) | string, onValue: (v: any) => void): void {
        if (_.isFunction(valueAccessor)) {
            onValue(valueAccessor(doc));
        } else {
            const property = valueAccessor as string;
            if (this.hasEntireValue(doc, property)) {
                onValue((doc as any)[property]);
                return;
            }

            if (this.fullDocumentsCache.has(doc.getId())) {
                const cachedValue = this.fullDocumentsCache.get(doc.getId()) as any;
                onValue(cachedValue[property]);
                return;
            }

            new getDocumentWithMetadataCommand(doc.getId(), this.db, true)
                .execute()
                .done((fullDocument: document) => {
                    onValue((fullDocument as any)[property]);
                })
                .fail((response: JQueryXHR) => {
                    messagePublisher.reportError("Failed to fetch document: " + doc.getId(), response.responseText);
                });
        }
    }

    private hasEntireValue(doc: document, property: string) {
        const meta = doc.__metadata as any;
        const arrays = meta[getDocumentsPreviewCommand.ArrayStubsKey];
        if (arrays && _.includes(arrays, property)) {
            return false;
        }

        const objects = meta[getDocumentsPreviewCommand.ObjectStubsKey];
        if (objects && _.includes(objects, property)) {
            return false;
        }

        const trimmed = meta[getDocumentsPreviewCommand.TrimmedValueKey];
        if (trimmed && _.includes(trimmed, property)) {
            return false;
        }
        return true;
    }
    
}

export = documentPropertyProvider;
