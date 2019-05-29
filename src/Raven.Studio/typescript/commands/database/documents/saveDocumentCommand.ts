import commandBase = require("commands/commandBase");
import document = require("models/database/documents/document");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class saveDocumentCommand extends commandBase {

    constructor(private id: string, private document: document, private db: database, private reportSaveProgress: boolean, private forceRevisionCreation: boolean) {
        super();
    }

    execute(): JQueryPromise<saveDocumentResponseDto> {
        this.document.__metadata.id = this.id;
        
        const commands: Array<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData> = [
            this.document.toBulkDoc("PUT")
        ];

        const args = ko.toJSON({ Commands: commands });
        
        const urlArgs = {
           forceRevisionCreation: this.forceRevisionCreation  
        };
              
        const url = endpoints.databases.batch.bulk_docs + this.urlEncodeArgs(urlArgs);
        
        const saveTask = this.post<saveDocumentResponseDto>(url, args, this.db);

        if (this.reportSaveProgress && !this.forceRevisionCreation) {
            saveTask.done((result: saveDocumentResponseDto) => this.reportSuccess("Saved " + result.Results[0]["@id"]));
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to save " + this.id, response.responseText, response.statusText));
        }

        if (this.reportSaveProgress && this.forceRevisionCreation) {
            saveTask.done((result: saveDocumentResponseDto) => this.reportSuccess("Created revision for" + result.Results[0]["@id"]));
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to create revision for: " + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = saveDocumentCommand;
