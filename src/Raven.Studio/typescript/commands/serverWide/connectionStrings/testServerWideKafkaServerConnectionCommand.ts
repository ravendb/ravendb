import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import KafkaConnectionSettings = Raven.Client.Documents.Operations.ETL.Queue.KafkaConnectionSettings;

type ConnectionOptionsDto = { [optionKey: string]: string };

export default class testServerWideKafkaServerConnectionCommand extends commandBase {
    constructor(
        private readonly bootstrapServers: string,
        private readonly useServerCertificate: boolean,
        private readonly connectionOptionsDto: ConnectionOptionsDto
    ) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const url = endpoints.global.queueEtlServerWideConnection.adminEtlQueueKafkaTestConnection;

        const payload: KafkaConnectionSettings = {
            BootstrapServers: this.bootstrapServers,
            ConnectionOptions: this.connectionOptionsDto,
            UseRavenCertificate: this.useServerCertificate,
        };

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), undefined, {
            dataType: undefined,
        })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Kafka server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Kafka server connection`, result.Error);
                }
            });
    }
}
