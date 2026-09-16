import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");

export default class testServerWideRabbitMqServerConnectionCommand extends commandBase {
    constructor(private readonly connectionString: string) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.queueEtlServerWideConnection.adminEtlQueueRabbitmqTestConnection;

        const payload = {
            ConnectionString: this.connectionString,
        };

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), undefined, {
            dataType: undefined,
        })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test RabbitMQ server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test RabbitMQ server connection`, result.Error);
                }
            });
    }
}
