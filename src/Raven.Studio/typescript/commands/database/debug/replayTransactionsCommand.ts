import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class replayTransactionsCommand extends commandBase {

    constructor(private db: database, private operationId: number, private file: File,
                private isUploading: KnockoutObservable<boolean>, private uploadStatus: KnockoutObservable<number>) {
        super();
    }
    
    execute(): JQueryPromise<Raven.Client.Documents.Operations.TransactionsRecording.ReplayTxOperationResult> {
        const urlArgs = {
            operationId: this.operationId
        };
        
        const url = endpoints.databases.transactionsRecording.transactionsReplay + this.urlEncodeArgs(urlArgs);
        
        const formData = new FormData();
        formData.append("file", this.file);
        
        return this.post<Raven.Client.Documents.Operations.TransactionsRecording.ReplayTxOperationResult>(url, formData, this.db, commandBase.getOptionsForImport(this.isUploading, this.uploadStatus), 0)
            .fail((response: JQueryXHR) => this.reportError("Failed to Replay Transaction Commands", response.responseText, response.statusText));
    }

}

export = replayTransactionsCommand;
