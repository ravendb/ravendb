import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class testServerWideAzureQueueStorageServerConnectionCommand extends commandBase {
    constructor(private readonly authentication: Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.queueEtlServerWideConnection.adminEtlQueueAzurequeuestorageTestConnection;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(this.authentication), undefined, {
            dataType: undefined,
        })
            .fail((response: JQueryXHR) =>
                this.reportError(`Failed to test Azure Queue Storage server connection`, response.responseText, response.statusText)
            )
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Azure Queue Storage server connection`, result.Error);
                }
            });
    }
}
