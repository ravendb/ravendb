import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");

class testKafkaServerConnectionCommand extends commandBase {

    constructor(private db: database, private kafkaServerUrl: string, private useServerCertificate: boolean, private connectionOptionsDto: {[optionKey: string]: string}) {
        super();
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {
        const args = {
            "bootstrap-servers": this.kafkaServerUrl
        };
        
        const url = endpoints.databases.queueEtlConnection.adminEtlQueueTestConnectionKafka + this.urlEncodeArgs(args);
        
        const payload = {
            Configuration: this.connectionOptionsDto,
            UseRavenCertificate: this.useServerCertificate
        }

        return this.post<Raven.Server.Web.System.NodeConnectionTestResult>(url, JSON.stringify(payload), this.db, { dataType: undefined })
            .fail((response: JQueryXHR) => this.reportError(`Failed to test Kafka server connection`, response.responseText, response.statusText))
            .done((result: Raven.Server.Web.System.NodeConnectionTestResult) => {
                if (!result.Success) {
                    this.reportError(`Failed to test Kafka server connection`, result.Error);
                }
            });
    }
}

export = testKafkaServerConnectionCommand;
