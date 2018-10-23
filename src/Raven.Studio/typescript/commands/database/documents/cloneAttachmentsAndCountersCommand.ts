import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class cloneAttachmentsAndCountersCommand extends commandBase {

    constructor(private sourceDocumentId: string, private fromRevision: boolean, private changeVector: string, private targetDocumentId: string, private db: database,
                private attachmentsToCopy: string[], private counters: Array<{ name: string, value: number }>) {
        super();
    }

    execute(): JQueryPromise<void> {
        const commands: Array<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData> = [
        ];
        
        this.attachmentsToCopy.forEach(attachment => {
            commands.push({
                Type: "AttachmentCOPY",
                Id: this.sourceDocumentId,
                AttachmentType: this.fromRevision ? "Revision" : "Document",
                ChangeVector: this.changeVector,
                Name: attachment,
                DestinationId: this.targetDocumentId,
                DestinationName: attachment
            } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData);
        });
        
        if (this.counters.length) {
            const operations = this.counters.map(c => {
                return {
                    Type: "Put", 
                    Delta: c.value,
                    CounterName: c.name
                } as Raven.Client.Documents.Operations.Counters.CounterOperation;
            });
            
            commands.push({
                Type: "Counters",
                Counters: {
                    DocumentId: this.targetDocumentId,
                    Operations: operations
                }
            } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData);
        }
        
        const args = ko.toJSON({ Commands: commands });
        const url = endpoints.databases.batch.bulk_docs;
        return this.post<void>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save attachments/counters for " + this.targetDocumentId, response.responseText, response.statusText));
    }
}

export = cloneAttachmentsAndCountersCommand;
