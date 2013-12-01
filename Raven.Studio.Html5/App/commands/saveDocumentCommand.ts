import commandBase = require("commands/commandBase");
import document = require("models/document");

class saveDocumentCommand extends commandBase {

    constructor(private id: string, private document: document) {
        super();
    }

    execute(): JQueryPromise<{ Key: string; ETag: string }> {
        var saveTask = this.ravenDb.saveDocument(this.id, this.document);

        this.reportInfo("Saving " + this.id + "...");

        saveTask.done(() => this.reportSuccess("Saved " + this.id));
        saveTask.fail((response) => this.reportError("Failed to save " + this.id, JSON.stringify(response)));
        return saveTask;
    }
}

export = saveDocumentCommand;