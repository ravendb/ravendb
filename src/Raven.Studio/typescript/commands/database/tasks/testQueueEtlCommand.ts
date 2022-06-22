import commandBase = require("commands/commandBase");
import database = require("models/resources/database");
import endpoints = require("endpoints");

class testQueueEtlCommand extends commandBase {
    constructor(private db: database, private payload: Raven.Server.Documents.ETL.Providers.Queue.Test.TestQueueEtlScript,
                private brokerType: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType) {
        super();
    }  

    execute(): JQueryPromise<Raven.Server.Documents.ETL.Providers.Queue.Test.QueueEtlTestScriptResult> {
        const url = endpoints.databases.queueEtl.adminEtlQueueTest;

        return this.post<Raven.Server.Documents.ETL.Providers.Queue.Test.QueueEtlTestScriptResult>(url, JSON.stringify(this.payload), this.db)
            .fail((response: JQueryXHR) => {
                this.reportError(`Failed to test ${this.brokerType} ETL`, response.responseText, response.statusText);
            });
    }
}

export = testQueueEtlCommand;
