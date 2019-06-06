import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class forceRevisionCreationCommand extends commandBase {

    constructor(private id: string, private db: database, private reportSaveProgress: boolean = true) {
        super();
    }

    execute(): JQueryPromise<saveDocumentResponseDto> {
        const toBulkDoc = {
                   Id: this.id,
                   Type: "ForceRevisionCreation"
        } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData;
        
        const commands: Array<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData> = [ toBulkDoc ];          

        const args = ko.toJSON({ Commands: commands });
        const url = endpoints.databases.batch.bulk_docs;
        
        const saveTask = this.post<saveDocumentResponseDto>(url, args, this.db);

        if (this.reportSaveProgress) {
            saveTask.done((result: saveDocumentResponseDto) => { 
                this.reportSuccess("Created revision for" + result.Results[0]["@id"])  
            });
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to create revision for: " + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = forceRevisionCreationCommand;
