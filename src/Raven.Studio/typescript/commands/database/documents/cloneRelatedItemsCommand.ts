import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class cloneRelatedItemsCommand extends commandBase {

    constructor(private sourceDocumentId: string, private fromRevision: boolean, private changeVector: string, private targetDocumentId: string, private db: database,
                private attachmentsToClone: string[],
                private timeseriesToClone: string[],
                private countersToClone: Array<{ name: string, value: number }>) {
        super();
    }

    execute(): JQueryPromise<void> {
        const commands: Array<Raven.Server.Documents.Handlers.BatchRequestParser.CommandData> = [
        ];

        this.attachmentsToClone.forEach(attachment => {
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

        this.timeseriesToClone.forEach(timeseries => {
            commands.push({
                Type: "TimeSeriesCopy",
                Id: this.sourceDocumentId,
                Name: timeseries,
                DestinationId: this.targetDocumentId,
                DestinationName: timeseries
            } as Raven.Server.Documents.Handlers.BatchRequestParser.CommandData);
        });

        if (this.countersToClone.length) {
            const operations = this.countersToClone.map(c => {
                return {
                    Type: "Increment",
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
            .fail((response: JQueryXHR) => this.reportError("Failed to save attachments/counters/timeseries for " + this.targetDocumentId, response.responseText, response.statusText));
    }
}

export = cloneRelatedItemsCommand;
