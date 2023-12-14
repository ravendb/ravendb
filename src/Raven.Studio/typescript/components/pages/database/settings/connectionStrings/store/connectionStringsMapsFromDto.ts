import {
    ConnectionStringUsedTask,
    ElasticSearchAuthenticationMethod,
    ElasticSearchConnection,
    KafkaConnection,
    OlapConnection,
    RabbitMqConnection,
    RavenConnection,
    SqlConnection,
} from "../connectionStringsTypes";
import OngoingTaskRavenEtl = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import { mapDestinationsFromDto } from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
type SqlConnectionStringDto = SqlConnectionString;

function getConnectionStringUsedTasks(
    tasks: Raven.Client.Documents.Operations.OngoingTasks.OngoingTask[],
    taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType,
    connectionName: string
): ConnectionStringUsedTask[] {
    return tasks
        .filter(
            (task: OngoingTaskRavenEtl) => task.TaskType === taskType && task.ConnectionStringName === connectionName
        )
        .map(
            (x) =>
                ({
                    id: x.TaskId,
                    name: x.TaskName,
                } satisfies ConnectionStringUsedTask)
        );
}

export function mapRavenConnectionsFromDto(
    connections: Record<string, RavenConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): RavenConnection[] {
    return Object.values(connections).map(
        (connection) =>
            ({
                type: "Raven",
                name: connection.Name,
                database: connection.Database,
                topologyDiscoveryUrls: connection.TopologyDiscoveryUrls.map((x) => ({ url: x })),
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "RavenEtl", connection.Name),
            } satisfies RavenConnection)
    );
}

export function mapSqlConnectionsFromDto(
    connections: Record<string, SqlConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): SqlConnection[] {
    return Object.values(connections).map(
        (connection) =>
            ({
                type: "Sql",
                name: connection.Name,
                connectionString: connection.ConnectionString,
                factoryName: connection.FactoryName,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "SqlEtl", connection.Name),
            } satisfies SqlConnection)
    );
}

export function mapOlapConnectionsFromDto(
    connections: Record<string, OlapConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): OlapConnection[] {
    return Object.values(connections).map(
        (connection) =>
            ({
                type: "Olap",
                name: connection.Name,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "OlapEtl", connection.Name),
                ...mapDestinationsFromDto(_.omit(connection, "Type", "Name")),
            } satisfies OlapConnection)
    );
}

function getElasticSearchAuthenticationMethod(
    dto: Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString
): ElasticSearchAuthenticationMethod {
    const auth = dto.Authentication;

    if (auth?.ApiKey?.EncodedApiKey) {
        return "Encoded API Key";
    }

    if (auth?.Basic?.Username && auth?.Basic?.Password) {
        return "Basic";
    }

    if (auth?.ApiKey?.ApiKeyId && auth?.ApiKey?.ApiKey) {
        return "API Key";
    }

    if (auth?.Certificate?.CertificatesBase64?.length > 0) {
        return "Certificate";
    }

    return "No authentication";
}

export function mapElasticSearchConnectionsFromDto(
    connections: Record<string, ElasticSearchConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): ElasticSearchConnection[] {
    return Object.values(connections).map(
        (connection) =>
            ({
                type: "ElasticSearch",
                name: connection.Name,
                authMethodUsed: getElasticSearchAuthenticationMethod(connection),
                apiKey: connection.Authentication?.ApiKey?.ApiKey,
                apiKeyId: connection.Authentication?.ApiKey?.ApiKeyId,
                username: connection.Authentication?.Basic?.Username,
                password: connection.Authentication?.Basic?.Password,
                certificatesBase64: connection.Authentication?.Certificate?.CertificatesBase64,
                nodes: connection.Nodes.map((x) => ({
                    url: x,
                })),
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "ElasticSearchEtl", connection.Name),
            } satisfies ElasticSearchConnection)
    );
}

export function mapKafkaConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): KafkaConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "Kafka")
        .map(
            (connection) =>
                ({
                    type: "Kafka",
                    name: connection.Name,
                    bootstrapServers: connection.KafkaConnectionSettings.BootstrapServers,
                    connectionOptions: Object.keys(connection.KafkaConnectionSettings.ConnectionOptions).map((key) => ({
                        key,
                        value: connection.KafkaConnectionSettings.ConnectionOptions[key],
                    })),
                    isUseRavenCertificate: connection.KafkaConnectionSettings.UseRavenCertificate,
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "QueueEtl", connection.Name),
                } satisfies KafkaConnection)
        );
}

export function mapRabbitMqConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTask[]
): RabbitMqConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "RabbitMq")
        .map(
            (connection) =>
                ({
                    type: "RabbitMQ",
                    name: connection.Name,
                    connectionString: connection.RabbitMqConnectionSettings.ConnectionString,
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, "QueueEtl", connection.Name),
                } satisfies RabbitMqConnection)
        );
}
