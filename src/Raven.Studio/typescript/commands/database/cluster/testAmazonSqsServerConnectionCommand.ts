import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class testAmazonSqsServerConnectionCommand extends commandBase {
    private readonly db: database | string;
    private readonly authentication: Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings; 

    constructor(db: database | string, authentication: Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings) {
        super();
        this.db = db;
        this.authentication = authentication;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.databases.queueEtlConnection.adminEtlQueueAmazonsqsTestConnection;

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(this.authentication), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Amazon SQS server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Amazon SQS server connection`, result.Error);
                }
            });
    }
}

export = testAmazonSqsServerConnectionCommand;
