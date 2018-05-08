import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteAttachmentCommand extends commandBase {

    constructor(private documentId: string, private attachmentName: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<void> {
        const args = {
            id: this.documentId,
            name: this.attachmentName
        };
        const url = endpoints.databases.attachment.attachments + this.urlEncodeArgs(args);
        return this.del<void>(url, null, this.db)
            .done(() => this.reportSuccess("Attachment was deleted."))
            .fail((response: JQueryXHR) => this.reportError("Failed to delete attachment: " + this.attachmentName, response.responseText, response.statusText));
    }
}

export = deleteAttachmentCommand;
