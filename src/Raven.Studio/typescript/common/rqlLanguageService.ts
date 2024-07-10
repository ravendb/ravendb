/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");
import remoteMetadataProvider = require("./autoComplete/remoteMetadataProvider");
import cachedMetadataProvider = require("./autoComplete/cachedMetadataProvider");
import { LanguageService } from "components/models/aceEditor";
import { DatabaseSharedInfo } from "components/models/databases";

class rqlLanguageService implements LanguageService {
    
    private nextMessageId = 1;
    private worker: Worker;
    private pendingMessages: Map<number, (response: LanguageServiceResponse) => void>;
    private latestSyntaxCheckRequestId = -1;
    private lastAutoCompleteRequestId = -1;
    private metadataProvider: queryCompleterProviders;
    private readonly queryType: rqlQueryType;
    
    constructor(
        db: database | DatabaseSharedInfo, 
        indexes: () => string[],
        queryType: rqlQueryType) {
        this.worker = new Worker("/studio/assets/rql_worker.js");
        this.queryType = queryType;
        
        this.metadataProvider = new cachedMetadataProvider(new remoteMetadataProvider(db, indexes));
        
        _.bindAll(this, "complete");
        
        this.pendingMessages = new Map<number, (response: LanguageServiceResponse) => void>();
        
        this.configure();
    }
    
    private configure() {
        this.worker.onmessage = ev => {
            this.handleMessage(ev);
        }
    }
    
    private postMessage(message: LanguageServiceRequest) {
        this.worker.postMessage(message);
    }
    
    private handleMessage(ev: MessageEvent) {
        if (ev.data.msgType === "response") {
            const response = ev.data as LanguageServiceResponse;

            const callback = this.pendingMessages.get(response.id);
            try {
                callback(response);
            } finally {
                this.pendingMessages.delete(response.id);
            }
        } 
        
        if (ev.data.msgType === "request") {
            const request = ev.data as LanguageServiceRequest;

            switch (request.type) {
                case "metadata":
                    this.handleMetadataRequest(request);
                    break;
            }
        }
    }
    
    private sendMetadataResponse(request: LanguageServiceMetadataRequest, payload: MetadataResponsePayload) {
        const response: LanguageServiceMetadataResponse = {
            id: request.id,
            msgType: "response",
            response: payload
        } 
        
        this.worker.postMessage(response);
    }
    
    private handleMetadataRequest(request: LanguageServiceMetadataRequest) {
        switch (request.payload.type) {
            case "collections":
                this.metadataProvider.collections(names => {
                    this.sendMetadataResponse(request, {
                        names
                    })
                });
                break;
            case "indexes":
                this.metadataProvider.indexNames(names => {
                    this.sendMetadataResponse(request, {
                        names
                    });
                });
                break;
            case "indexFields": 
                {
                    const payload = request.payload as MetadataRequestListIndexFields;
                    this.metadataProvider.indexFields(payload.indexName, fields => {
                        this.sendMetadataResponse(request, {
                            fields
                        });
                    })
                }
                break;
            case "collectionFields":
                {
                    const payload = request.payload as MetadataRequestListCollectionFields;
                    this.metadataProvider.collectionFields(payload.collectionName, payload.prefix, fields => {
                        this.sendMetadataResponse(request, {
                            fields
                        });
                    })
                }
                break;
            
            default:
                throw new Error("Unhandled metadata request" + request.payload);
        }
    }
    
    private static getEditorText(editor: AceAjax.Editor) {
        return editor
            .getSession()
            .getDocument()
            .getAllLines()
            .join("\r\n");
    }
    
    syntaxCheck(editor: AceAjax.Editor) {
        const text = rqlLanguageService.getEditorText(editor);
        
        const requestId = this.nextMessageId++;
        
        this.latestSyntaxCheckRequestId = requestId;
        
        this.postMessage({
            msgType: "request",
            type: "syntax",
            id: requestId,
            query: text,
            queryType: this.queryType
        });
        
        this.pendingMessages.set(requestId, (response: LanguageServiceSyntaxResponse) => {
            if (requestId === this.latestSyntaxCheckRequestId) {
                editor.getSession().setAnnotations(response.annotations);
            }
        });
    }

    complete(editor: AceAjax.Editor,
             session: AceAjax.IEditSession,
             pos: AceAjax.Position,
             prefix: string,
             callback: (errors: any[], wordList: autoCompleteWordList[]) => void) {
        const text = rqlLanguageService.getEditorText(editor);
        
        const requestId = this.nextMessageId++;
        
        this.lastAutoCompleteRequestId = requestId;
        
        this.postMessage({
            msgType: "request",
            type: "complete",
            id: requestId,
            queryType: this.queryType,
            query: text,
            position: {
                row: pos.row,
                column: pos.column
            }
        });
        
        this.pendingMessages.set(requestId, (response: LanguageServiceAutoCompleteResponse) => {
            if (requestId == this.lastAutoCompleteRequestId) {
                callback(null, response.wordList);
            }
        });
    }
    
    dispose() {
        this.worker.terminate();
    }
}

export = rqlLanguageService;
