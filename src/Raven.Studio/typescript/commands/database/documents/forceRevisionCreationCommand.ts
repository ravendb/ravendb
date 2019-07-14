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
                if (result.Results[0].RevisionCreated) {
                    this.reportSuccess("Created revision for document:" + result.Results[0]["@id"]);
                }
                else {
                    this.reportSuccess("No new revision created. A revision with the latest document content already exists");
                }
            });            
            saveTask.fail((response: JQueryXHR) => this.reportError("Failed to create revision for document: " + this.id, response.responseText, response.statusText));
        }

        return saveTask;
    }
}

export = forceRevisionCreationCommand;
