import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class deleteCounterCommand extends commandBase {

    constructor(private counterName: string, private documentId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Counters.CounterBatch> {

        const payload = {
            ReplyWithAllNodesValues: true,
            Documents:
                [{
                    DocumentId: this.documentId,
                    Operations:
                        [{
                            Type: 'Delete',
                            CounterName: this.counterName
                        }]
                }]
        }  as Raven.Client.Documents.Operations.Counters.CounterBatch; 

        const url = endpoints.databases.counters.counters;

        return this.post<Raven.Client.Documents.Operations.Counters.CountersDetail>(url, JSON.stringify(payload), this.db)
            .done(() => {
                this.reportSuccess("Counter deleted successfully: " + this.counterName);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to delete counter", response.responseText, response.statusText));
    }
}

export = deleteCounterCommand;
