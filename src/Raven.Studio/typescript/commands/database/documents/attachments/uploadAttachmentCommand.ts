import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class uploadAttachmentCommand extends commandBase {

    private xhr: XMLHttpRequest;
    
    constructor(private file: File, private documentId: string, private db: database, private onProgress?: (event: ProgressEvent) => void) {
        super();
    }
    
    abort() {
        if (this.xhr) {
            this.xhr.abort();    
        }
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Attachments.AttachmentDetails> {
        const args = {
            id: this.documentId,
            name: this.file.name,
            contentType: this.file.type
        };

        const options: JQueryAjaxSettings = {
            processData: false,
            cache: false,
            dataType: '',
            xhr: () => {
                const xhr = new XMLHttpRequest();
                xhr.upload.addEventListener("progress", (evt: ProgressEvent) => {
                    if (this.onProgress) {
                        this.onProgress(evt);
                    }
                }, false);
                
                this.xhr = xhr;
                return xhr;
            }
        };

        const url = endpoints.databases.attachment.attachments + this.urlEncodeArgs(args);
        return this.put<Raven.Client.Documents.Operations.Attachments.AttachmentDetails>(url, this.file, this.db, options, 0)
            .done(() => {
                this.reportSuccess("Successfully uploaded attachment: " + this.file.name);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to upload attachment", response.responseText, response.statusText));
    }
}

export = uploadAttachmentCommand;
