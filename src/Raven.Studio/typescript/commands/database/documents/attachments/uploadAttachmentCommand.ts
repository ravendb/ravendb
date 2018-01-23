import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class uploadAttachmentCommand extends commandBase {

    constructor(private file: File, private documentId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Attachments.AttachmentDetails> {
        const args = {
            id: this.documentId,
            name: this.file.name,
            contentType: this.file.type
        };

        //TODO: progress support, see: https://github.com/ravendb/ravendb/blob/v3.5/Raven.Studio.Html5/App/commands/filesystem/uploadFileToFilesystemCommand.ts

        const options: JQueryAjaxSettings = {
            processData: false,
            cache: false,
            dataType: ''
        }

        const url = endpoints.databases.attachment.attachments + this.urlEncodeArgs(args);
        return this.put<Raven.Client.Documents.Operations.Attachments.AttachmentDetails>(url, this.file, this.db, options, 0)
            .done(() => {
                this.reportSuccess("Successfully uploaded attachment: " + this.file.name);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to upload attachment", response.responseText, response.statusText));
    }
}

export = uploadAttachmentCommand;
