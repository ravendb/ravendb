import commandBase = require("commands/commandBase");
import document = require("models/document");
import database = require("models/database");

class saveDocumentCommand extends commandBase {

    constructor(private id: string, private document: document, private db: database) {
        super();
    }

    execute(): JQueryPromise<{ Key: string; ETag: string }> {
        this.reportInfo("Saving " + this.id + "...");

        var customHeaders = {
            'Raven-Client-Version': commandBase.ravenClientVersion,
            'Raven-Entity-Name': this.document.__metadata.ravenEntityName,
            'Raven-Clr-Type': this.document.__metadata.ravenClrType,
            'If-None-Match': this.document.__metadata.etag
        };
        var jQueryOptions: JQueryAjaxSettings = {
            headers: <any>customHeaders
        };
        var args = JSON.stringify(this.document.toDto());
        var url = "/docs/" + this.id;
        var saveTask = this.put(url, args, this.db, jQueryOptions);

        saveTask.done(() => this.reportSuccess("Saved " + this.id));
        saveTask.fail((response) => this.reportError("Failed to save " + this.id, JSON.stringify(response)));
        return saveTask;
    }
}

export = saveDocumentCommand;