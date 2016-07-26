import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDocumentCommand extends commandBase {

    constructor(private id: string, private document: document, private db: database, private reportSaveProgress = true) {
        super();
    }

    execute(): JQueryPromise<bulkDocumentDto[]> {
        if (this.reportSaveProgress) {
            this.reportInfo("Saving " + this.id + "...");
        }

        var commands: Array<bulkDocumentDto> = [this.document.toBulkDoc("PUT")];

        var args = ko.toJSON(commands);
        var url = endpoints.databases.batch.bulk_docs;
        var saveTask = this.post(url, args, this.db);

        if (this.reportSaveProgress) {
            saveTask.done(() => this.reportSuccess("Saved " + this.id));
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save " + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = saveDocumentCommand;
