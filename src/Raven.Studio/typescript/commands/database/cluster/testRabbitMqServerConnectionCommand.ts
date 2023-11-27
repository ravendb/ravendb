import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class testRabbitMqServerConnectionCommand extends commandBase {
    private readonly db: database;
    private readonly connectionString: string;

    constructor(db: database, connectionString: string) {
        super();
        this.db = db;
        this.connectionString = connectionString;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {

        const url = endpoints.databases.queueEtlConnection.adminEtlQueueRabbitmqTestConnection

        const payload = {
            ConnectionString: this.connectionString
        }

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test RabbitMQ server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test RabbitMQ server connection`, result.Error);
                }
            });
    }
}

export = testRabbitMqServerConnectionCommand;
