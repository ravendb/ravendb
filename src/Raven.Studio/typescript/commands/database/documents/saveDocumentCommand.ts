import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDocumentCommand extends commandBase {
    constructor(
        private id: string,
        private document: document,
        private db: database,
        private reportSaveProgress: boolean = true,
        private transactionMode: Raven.Client.Documents.Session.TransactionMode = "SingleNode"
    ) {
        super();
    }

    execute(): JQueryPromise<saveDocumentResponseDto> {
        this.document.__metadata.id = this.id;
        
        const commands: Array<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData> = [
            this.document.toBulkDoc("PUT")
        ];

        const args = JSON.stringify({
            Commands: commands,
            TransactionMode: this.transactionMode,
        });
        const url = endpoints.databases.batch.bulk_docs;
        
        const saveTask = this.post<saveDocumentResponseDto>(url, args, this.db);

        if (this.reportSaveProgress) {
            saveTask.done((result: saveDocumentResponseDto) => this.reportSuccess("Saved document: " + result.Results[0]["@id"]));
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save document:" + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = saveDocumentCommand;
