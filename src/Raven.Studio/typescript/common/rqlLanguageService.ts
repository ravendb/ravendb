/// <reference path="../../typings/tsd.d.ts" />

import database = require("models/resources/database");

class rqlLanguageService {
    
    private nextMessageId = 1;
    private worker: Worker;
    private pendingMessages: Map<number, Function>;
    private latestSyntaxCheckRequestId = -1;
    private lastAutoCompleteRequestId = -1;
    
    constructor(activeDatabase: KnockoutObservable<database>, indexes: KnockoutObservableArray<Raven.Client.Documents.Operations.IndexInformation>) {
        this.worker = new Worker("/studio/rql_worker.js");
        
        _.bindAll(this, "complete");
        
        this.pendingMessages = new Map<number, Function>();
        
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
        const response = ev.data as LanguageServiceResponse;
        
        const callback = this.pendingMessages.get(response.id);
        try {
            callback(response);
        } finally {
            this.pendingMessages.delete(response.id);
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
            type: "syntax",
            id: requestId,
            data: text
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
            type: "complete",
            id: requestId,
            data: text,
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
