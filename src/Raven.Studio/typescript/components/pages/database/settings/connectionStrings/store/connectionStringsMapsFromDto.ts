import {
    ConnectionStringUsedTask,
    ElasticSearchAuthenticationMethod,
    ElasticSearchConnection,
    KafkaConnection,
    OlapConnection,
    RabbitMqConnection,
    AzureQueueStorageConnection,
    RavenConnection,
    SqlConnection,
    SnowflakeConnection,
} from "../connectionStringsTypes";

import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import { mapDestinationsFromDto } from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import assertUnreachable from "components/utils/assertUnreachable";

type SqlConnectionStringDto = SqlConnectionString;
type SnowflakeConnectionStringDto = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString;

type OngoingTaskForConnection = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask & {
    ConnectionStringName?: string;
    BrokerType?: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType;
};

function getConnectionStringUsedTasks(
    tasks: OngoingTaskForConnection[],
    connectionType: StudioEtlType,
    connectionName: string
): ConnectionStringUsedTask[] {
    let filteredTasks: OngoingTaskForConnection[] = [];

    switch (connectionType) {
        case "Raven":
            filteredTasks = tasks.filter((task) =>
                ["RavenEtl", "Replication", "PullReplicationAsSink"].includes(task.TaskType)
            );
            break;
        case "Sql":
            filteredTasks = tasks.filter((task) => task.TaskType === "SqlEtl");
            break;
        case "Snowflake":
            filteredTasks = tasks.filter((task) => task.TaskType === "SnowflakeEtl");
            break;
        case "Olap":
            filteredTasks = tasks.filter((task) => task.TaskType === "OlapEtl");
            break;
        case "ElasticSearch":
            filteredTasks = tasks.filter((task) => task.TaskType === "ElasticSearchEtl");
            break;
        case "RabbitMQ":
            filteredTasks = tasks.filter((task) => task.BrokerType === "RabbitMq");
            break;
        case "Kafka":
            filteredTasks = tasks.filter((task) => task.BrokerType === "Kafka");
            break;
        case "AzureQueueStorage":
            filteredTasks = tasks.filter((task) => task.BrokerType === "AzureQueueStorage");
            break;
        default:
            assertUnreachable(connectionType);
    }

    filteredTasks = filteredTasks.filter((task) => task.ConnectionStringName === connectionName);

    return filteredTasks.map(
        (x) =>
            ({
                id: x.TaskId,
                name: x.TaskName,
            }) satisfies ConnectionStringUsedTask
    );
}

export function mapRavenConnectionsFromDto(
    connections: Record<string, RavenConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): RavenConnection[] {
    const type: RavenConnection["type"] = "Raven";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
                name: connection.Name,
                database: connection.Database,
                topologyDiscoveryUrls: connection.TopologyDiscoveryUrls.map((x) => ({ url: x })),
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
            }) satisfies RavenConnection
    );
}

export function mapSqlConnectionsFromDto(
    connections: Record<string, SqlConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): SqlConnection[] {
    const type: SqlConnection["type"] = "Sql";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
                name: connection.Name,
                connectionString: connection.ConnectionString,
                factoryName: connection.FactoryName,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
            }) satisfies SqlConnection
    );
}

export function mapSnowflakeConnectionsFromDto(
    connections: Record<string, SnowflakeConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): SnowflakeConnection[] {
    const type: SnowflakeConnection["type"] = "Snowflake";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
                name: connection.Name,
                connectionString: connection.ConnectionString,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
            }) satisfies SnowflakeConnection
    );
}

export function mapOlapConnectionsFromDto(
    connections: Record<string, OlapConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): OlapConnection[] {
    const type: OlapConnection["type"] = "Olap";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
                name: connection.Name,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                ...mapDestinationsFromDto(_.omit(connection, "Type", "Name")),
            }) satisfies OlapConnection
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
    ongoingTasks: OngoingTaskForConnection[]
): ElasticSearchConnection[] {
    const type: ElasticSearchConnection["type"] = "ElasticSearch";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
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
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
            }) satisfies ElasticSearchConnection
    );
}

export function mapKafkaConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): KafkaConnection[] {
    const type: KafkaConnection["type"] = "Kafka";

    return Object.values(connections)
        .filter((x) => x.BrokerType === "Kafka")
        .map(
            (connection) =>
                ({
                    type,
                    name: connection.Name,
                    bootstrapServers: connection.KafkaConnectionSettings.BootstrapServers,
                    connectionOptions: Object.keys(connection.KafkaConnectionSettings.ConnectionOptions).map((key) => ({
                        key,
                        value: connection.KafkaConnectionSettings.ConnectionOptions[key],
                    })),
                    isUseRavenCertificate: connection.KafkaConnectionSettings.UseRavenCertificate,
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                }) satisfies KafkaConnection
        );
}

export function mapRabbitMqConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): RabbitMqConnection[] {
    const type: RabbitMqConnection["type"] = "RabbitMQ";

    return Object.values(connections)
        .filter((x) => x.BrokerType === "RabbitMq")
        .map(
            (connection) =>
                ({
                    type,
                    name: connection.Name,
                    connectionString: connection.RabbitMqConnectionSettings.ConnectionString,
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                }) satisfies RabbitMqConnection
        );
}

function getAzureQueueStorageAuthType(dto: QueueConnectionStringDto): AzureQueueStorageAuthenticationType {
    if (dto.AzureQueueStorageConnectionSettings.ConnectionString) {
        return "connectionString";
    }
    if (dto.AzureQueueStorageConnectionSettings.EntraId) {
        return "entraId";
    }
    if (dto.AzureQueueStorageConnectionSettings.Passwordless) {
        return "passwordless";
    }
}

export function mapAzureQueueStorageConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): AzureQueueStorageConnection[] {
    const type: AzureQueueStorageConnection["type"] = "AzureQueueStorage";

    return Object.values(connections)
        .filter((x) => x.BrokerType === "AzureQueueStorage")
        .map(
            (connection) =>
                ({
                    type,
                    name: connection.Name,
                    authType: getAzureQueueStorageAuthType(connection),
                    settings: {
                        connectionString: {
                            connectionStringValue: connection.AzureQueueStorageConnectionSettings.ConnectionString,
                        },
                        entraId: {
                            clientId: connection.AzureQueueStorageConnectionSettings.EntraId?.ClientId,
                            clientSecret: connection.AzureQueueStorageConnectionSettings.EntraId?.ClientSecret,
                            storageAccountName:
                                connection.AzureQueueStorageConnectionSettings.EntraId?.StorageAccountName,
                            tenantId: connection.AzureQueueStorageConnectionSettings.EntraId?.TenantId,
                        },
                        passwordless: {
                            storageAccountName:
                                connection.AzureQueueStorageConnectionSettings.Passwordless?.StorageAccountName,
                        },
                    },
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                }) satisfies AzureQueueStorageConnection
        );
}
