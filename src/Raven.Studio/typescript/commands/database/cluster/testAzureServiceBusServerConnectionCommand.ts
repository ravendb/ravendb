import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class testAzureServiceBusServerConnectionCommand extends commandBase {
    constructor(private readonly db: database | string, private readonly settings: Raven.Client.Documents.Operations.ETL.Queue.AzureServiceBusConnectionSettings) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.databases.queueEtlConnection.adminEtlQueueAzureservicebusTestConnection;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(this.settings), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Azure Service Bus server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Azure Service Bus server connection`, result.Error);
                }
            });
    }
}

export = testAzureServiceBusServerConnectionCommand;
