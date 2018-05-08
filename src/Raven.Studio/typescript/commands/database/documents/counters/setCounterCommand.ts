import commandBase = require("commands/commandBase");
import database = require("models/resources/database")
import endpoints = require("endpoints");

class setCounterCommand extends commandBase {

    constructor(private counterName: string, private deltaValue: number, private documentId: string, private db: database) {
        super();
    }

    execute(): JQueryPromise<Raven.Client.Documents.Operations.Counters.CountersDetail> {

        const payload = {
            ReplyWithAllNodesValues: true,
            Documents:
                [{
                    DocumentId: this.documentId,
                    Operations:
                        [{
                            Type: 'Increment',
                            CounterName: this.counterName,
                            Delta: this.deltaValue
                        }]
                }]
        } as Raven.Client.Documents.Operations.Counters.CounterBatch;

        const url = endpoints.databases.counters.counters;

        return this.post<Raven.Client.Documents.Operations.Counters.CountersDetail>(url, JSON.stringify(payload), this.db, {dataType: undefined})
            .done(() => {
                this.reportSuccess("Counter was set successfully: " + this.counterName);
            })
            .fail((response: JQueryXHR) => this.reportError("Failed to set counter", response.responseText, response.statusText));
    }
}

export = setCounterCommand;
