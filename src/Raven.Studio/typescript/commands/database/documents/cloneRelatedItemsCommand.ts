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
        const commands: Array<Partial<Raven.Server.Documents.Handlers.Batches.BatchRequestParser.CommandData>> = [
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
            });
        });

        this.timeseriesToClone.forEach(timeseries => {
            commands.push({
                Type: "TimeSeriesCopy",
                Id: this.sourceDocumentId,
                Name: timeseries,
                DestinationId: this.targetDocumentId,
                DestinationName: timeseries
            });
        });

        if (this.countersToClone.length) {
            const operations: Raven.Client.Documents.Operations.Counters.CounterOperation[] = this.countersToClone.map(c => {
                return {
                    Type: "Increment",
                    Delta: c.value,
                    CounterName: c.name
                };
            });

            commands.push({
                Type: "Counters",
                Counters: {
                    DocumentId: this.targetDocumentId,
                    Operations: operations
                }
            });
        }

        const args = ko.toJSON({ Commands: commands });
        const url = endpoints.databases.batch.bulk_docs;
        return this.post<void>(url, args, this.db)
            .fail((response: JQueryXHR) => this.reportError("Failed to save attachments/counters/timeseries for " + this.targetDocumentId, response.responseText, response.statusText));
    }
}

export = cloneRelatedItemsCommand;
