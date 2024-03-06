import commandBase = require("commands/commandBase");
import endpoints = require("endpoints");
import database = require("models/resources/database");
import KafkaConnectionSettings = Raven.Client.Documents.Operations.ETL.Queue.KafkaConnectionSettings;

type ConnectionOptionsDto = {[optionKey: string]: string};

class testKafkaServerConnectionCommand extends commandBase {
    private readonly db: database;
    private readonly bootstrapServers: string;
    private readonly useServerCertificate: boolean;
    private readonly connectionOptionsDto: ConnectionOptionsDto;

    constructor(db: database, bootstrapServers: string, useServerCertificate: boolean, connectionOptionsDto: ConnectionOptionsDto) {
        super();
        this.db = db;
        this.bootstrapServers = bootstrapServers;
        this.useServerCertificate = useServerCertificate;
        this.connectionOptionsDto = connectionOptionsDto;
    }

    execute(): JQueryPromise<Raven.Server.Web.System.NodeConnectionTestResult> {

        const url = endpoints.databases.queueEtlConnection.adminEtlQueueKafkaTestConnection

        const payload: KafkaConnectionSettings = {
            BootstrapServers: this.bootstrapServers,
            ConnectionOptions: this.connectionOptionsDto,
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
