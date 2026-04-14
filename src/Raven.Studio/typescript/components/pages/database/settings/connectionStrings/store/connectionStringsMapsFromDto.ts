import {
    Connection,
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
    AmazonSqsConnection,
    AzureServiceBusConnection,
    AiConnection,
    StudioConnectionType,
} from "../connectionStringsTypes";

import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import { mapDestinationsFromDto } from "components/common/formDestinations/utils/formDestinationsMapsFromDto";
import assertUnreachable from "components/utils/assertUnreachable";

type SqlConnectionStringDto = SqlConnectionString;
type SnowflakeConnectionStringDto = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString;
type AiConnectionStringDto = Raven.Client.Documents.Operations.AI.AiConnectionString;

type OngoingTaskForConnection = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask & {
    ConnectionStringName?: string;
    BrokerType?: Raven.Client.Documents.Operations.ETL.Queue.QueueBrokerType;
};

function getConnectionStringUsedTasks(
    tasks: OngoingTaskForConnection[],
    connectionType: StudioConnectionType,
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
        case "AmazonSqs":
            filteredTasks = tasks.filter((task) => task.BrokerType === "AmazonSqs");
            break;
        case "AzureServiceBus":
            filteredTasks = tasks.filter((task) => task.BrokerType === "AzureServiceBus");
            break;
        case "Ai":
            filteredTasks = tasks.filter((task) => task.TaskType === "EmbeddingsGeneration");
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

export function mapAmazonSqsConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): AmazonSqsConnection[] {
    const type: AmazonSqsConnection["type"] = "AmazonSqs";

    return Object.values(connections)
        .filter((x) => x.BrokerType === "AmazonSqs")
        .map(
            (connection) =>
                ({
                    type,
                    name: connection.Name,
                    authType: getAmazonSqsAuthType(connection),
                    settings: {
                        passwordless: connection.AmazonSqsConnectionSettings.Passwordless,
                        basic: {
                            accessKey: connection.AmazonSqsConnectionSettings.Basic?.AccessKey,
                            secretKey: connection.AmazonSqsConnectionSettings.Basic?.SecretKey,
                            regionName: connection.AmazonSqsConnectionSettings.Basic?.RegionName,
                        },
                    },
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                }) satisfies AmazonSqsConnection
        );
}

function getAmazonSqsAuthType(dto: QueueConnectionStringDto): AmazonSqsAuthenticationType {
    if (dto.AmazonSqsConnectionSettings.Passwordless) {
        return "passwordless";
    }
    if (dto.AmazonSqsConnectionSettings.Basic) {
        return "basic";
    }
    return null;
}

function getAzureServiceBusAuthType(dto: QueueConnectionStringDto): AzureServiceBusAuthenticationType {
    if (dto.AzureServiceBusConnectionSettings.ConnectionString) {
        return "connectionString";
    }
    if (dto.AzureServiceBusConnectionSettings.EntraId) {
        return "entraId";
    }
    if (dto.AzureServiceBusConnectionSettings.Passwordless) {
        return "passwordless";
    }
}

export function mapAzureServiceBusConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): AzureServiceBusConnection[] {
    const type: AzureServiceBusConnection["type"] = "AzureServiceBus";

    return Object.values(connections)
        .filter((x) => x.BrokerType === "AzureServiceBus")
        .map(
            (connection) =>
                ({
                    type,
                    name: connection.Name,
                    authType: getAzureServiceBusAuthType(connection),
                    settings: {
                        connectionString: {
                            connectionStringValue: connection.AzureServiceBusConnectionSettings.ConnectionString,
                        },
                        entraId: {
                            namespace: connection.AzureServiceBusConnectionSettings.EntraId?.Namespace,
                            tenantId: connection.AzureServiceBusConnectionSettings.EntraId?.TenantId,
                            clientId: connection.AzureServiceBusConnectionSettings.EntraId?.ClientId,
                            clientSecret: connection.AzureServiceBusConnectionSettings.EntraId?.ClientSecret,
                        },
                        passwordless: {
                            namespace: connection.AzureServiceBusConnectionSettings.Passwordless?.Namespace,
                        },
                    },
                    usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                }) satisfies AzureServiceBusConnection
        );
}

function getAiConnectorType(connection: AiConnectionStringDto): AiConnection["connectorType"] {
    if (connection.AzureOpenAiSettings) {
        return "azureOpenAiSettings";
    }
    if (connection.GoogleSettings) {
        return "googleSettings";
    }
    if (connection.HuggingFaceSettings) {
        return "huggingFaceSettings";
    }
    if (connection.OllamaSettings) {
        return "ollamaSettings";
    }
    if (connection.EmbeddedSettings) {
        return "embeddedSettings";
    }
    if (connection.OpenAiSettings) {
        return "openAiSettings";
    }
    if (connection.MistralAiSettings) {
        return "mistralAiSettings";
    }
    if (connection.VertexSettings) {
        return "vertexSettings";
    }
    return null;
}

type WithExcludedDatabases<T> = T & { ExcludedDatabases?: string[] };

export type ServerWideConnectionStringDto =
    | WithExcludedDatabases<RavenConnectionStringDto>
    | WithExcludedDatabases<SqlConnectionStringDto>
    | WithExcludedDatabases<SnowflakeConnectionStringDto>
    | WithExcludedDatabases<OlapConnectionStringDto>
    | WithExcludedDatabases<ElasticSearchConnectionStringDto>
    | WithExcludedDatabases<QueueConnectionStringDto>
    | WithExcludedDatabases<AiConnectionStringDto>;

const noTasks: ConnectionStringUsedTask[] = [];


export function mapServerWideConnectionsFromDto(
    results: ServerWideConnectionStringDto[]
): { [key in StudioConnectionType]: Connection[] } {
    const mapped: Record<StudioConnectionType, Connection[]> = {
        Raven: [],
        Sql: [],
        Snowflake: [],
        Olap: [],
        ElasticSearch: [],
        Kafka: [],
        RabbitMQ: [],
        AzureQueueStorage: [],
        AmazonSqs: [],
        Ai: [],
    };

    for (const dto of results) {
        const excludedDatabases = dto.ExcludedDatabases ?? [];
        switch (dto.Type) {
            case "Raven": {
                const d = dto as WithExcludedDatabases<RavenConnectionStringDto>;
                mapped.Raven.push({
                    type: "Raven",
                    name: d.Name,
                    database: d.Database,
                    topologyDiscoveryUrls: d.TopologyDiscoveryUrls.map((url) => ({ url })),
                    usedByTasks: noTasks,
                    excludedDatabases,
                } satisfies RavenConnection);
                break;
            }
            case "Sql": {
                const d = dto as WithExcludedDatabases<SqlConnectionStringDto>;
                mapped.Sql.push({
                    type: "Sql",
                    name: d.Name,
                    connectionString: d.ConnectionString,
                    factoryName: d.FactoryName,
                    usedByTasks: noTasks,
                    excludedDatabases,
                } satisfies SqlConnection);
                break;
            }
            case "Snowflake": {
                const d = dto as WithExcludedDatabases<SnowflakeConnectionStringDto>;
                mapped.Snowflake.push({
                    type: "Snowflake",
                    name: d.Name,
                    connectionString: d.ConnectionString,
                    usedByTasks: noTasks,
                    excludedDatabases,
                } satisfies SnowflakeConnection);
                break;
            }
            case "Olap": {
                const d = dto as WithExcludedDatabases<OlapConnectionStringDto>;
                mapped.Olap.push({
                    type: "Olap",
                    name: d.Name,
                    usedByTasks: noTasks,
                    excludedDatabases,
                    ...mapDestinationsFromDto(_.omit(d, "Type", "Name")),
                } satisfies OlapConnection);
                break;
            }
            case "ElasticSearch": {
                const d = dto as WithExcludedDatabases<ElasticSearchConnectionStringDto>;
                mapped.ElasticSearch.push({
                    type: "ElasticSearch",
                    name: d.Name,
                    authMethodUsed: getElasticSearchAuthenticationMethod(d),
                    apiKey: d.Authentication?.ApiKey?.ApiKey,
                    apiKeyId: d.Authentication?.ApiKey?.ApiKeyId,
                    username: d.Authentication?.Basic?.Username,
                    password: d.Authentication?.Basic?.Password,
                    certificatesBase64: d.Authentication?.Certificate?.CertificatesBase64,
                    nodes: d.Nodes.map((url) => ({ url })),
                    usedByTasks: noTasks,
                    excludedDatabases,
                } satisfies ElasticSearchConnection);
                break;
            }
            case "Queue": {
                const d = dto as WithExcludedDatabases<QueueConnectionStringDto>;
                switch (d.BrokerType) {
                    case "Kafka":
                        mapped.Kafka.push({
                            type: "Kafka",
                            name: d.Name,
                            bootstrapServers: d.KafkaConnectionSettings.BootstrapServers,
                            connectionOptions: Object.keys(d.KafkaConnectionSettings.ConnectionOptions).map((key) => ({
                                key,
                                value: d.KafkaConnectionSettings.ConnectionOptions[key],
                            })),
                            isUseRavenCertificate: d.KafkaConnectionSettings.UseRavenCertificate,
                            usedByTasks: noTasks,
                            excludedDatabases,
                        } satisfies KafkaConnection);
                        break;
                    case "RabbitMq":
                        mapped.RabbitMQ.push({
                            type: "RabbitMQ",
                            name: d.Name,
                            connectionString: d.RabbitMqConnectionSettings.ConnectionString,
                            usedByTasks: noTasks,
                            excludedDatabases,
                        } satisfies RabbitMqConnection);
                        break;
                    case "AzureQueueStorage":
                        mapped.AzureQueueStorage.push({
                            type: "AzureQueueStorage",
                            name: d.Name,
                            authType: getAzureQueueStorageAuthType(d),
                            settings: {
                                connectionString: {
                                    connectionStringValue: d.AzureQueueStorageConnectionSettings.ConnectionString,
                                },
                                entraId: {
                                    clientId: d.AzureQueueStorageConnectionSettings.EntraId?.ClientId,
                                    clientSecret: d.AzureQueueStorageConnectionSettings.EntraId?.ClientSecret,
                                    storageAccountName: d.AzureQueueStorageConnectionSettings.EntraId?.StorageAccountName,
                                    tenantId: d.AzureQueueStorageConnectionSettings.EntraId?.TenantId,
                                },
                                passwordless: {
                                    storageAccountName: d.AzureQueueStorageConnectionSettings.Passwordless?.StorageAccountName,
                                },
                            },
                            usedByTasks: noTasks,
                            excludedDatabases,
                        } satisfies AzureQueueStorageConnection);
                        break;
                    case "AmazonSqs":
                        mapped.AmazonSqs.push({
                            type: "AmazonSqs",
                            name: d.Name,
                            authType: getAmazonSqsAuthType(d),
                            settings: {
                                passwordless: d.AmazonSqsConnectionSettings.Passwordless,
                                basic: {
                                    accessKey: d.AmazonSqsConnectionSettings.Basic?.AccessKey,
                                    secretKey: d.AmazonSqsConnectionSettings.Basic?.SecretKey,
                                    regionName: d.AmazonSqsConnectionSettings.Basic?.RegionName,
                                },
                            },
                            usedByTasks: noTasks,
                            excludedDatabases,
                        } satisfies AmazonSqsConnection);
                        break;
                }
                break;
            }
            case "Ai": {
                const d = dto as WithExcludedDatabases<AiConnectionStringDto>;
                mapped.Ai.push({
                    type: "Ai",
                    name: d.Name,
                    usedByTasks: noTasks,
                    excludedDatabases,
                    identifier: d.Identifier,
                    connectorType: getAiConnectorType(d),
                    modelType: d.ModelType,
                    azureOpenAiSettings: {
                        apiKey: d.AzureOpenAiSettings?.ApiKey,
                        endpoint: d.AzureOpenAiSettings?.Endpoint,
                        model: d.AzureOpenAiSettings?.Model,
                        deploymentName: d.AzureOpenAiSettings?.DeploymentName,
                        dimensions: d.AzureOpenAiSettings?.Dimensions,
                        embeddingsMaxConcurrentBatches: d.AzureOpenAiSettings?.EmbeddingsMaxConcurrentBatches,
                        isSetTemperature: d.AzureOpenAiSettings?.Temperature != null,
                        temperature: d.AzureOpenAiSettings?.Temperature ?? null,
                    },
                    googleSettings: {
                        aiVersion: d.GoogleSettings?.AiVersion,
                        apiKey: d.GoogleSettings?.ApiKey,
                        model: d.GoogleSettings?.Model,
                        dimensions: d.GoogleSettings?.Dimensions,
                        embeddingsMaxConcurrentBatches: d.GoogleSettings?.EmbeddingsMaxConcurrentBatches,
                    },
                    huggingFaceSettings: {
                        apiKey: d.HuggingFaceSettings?.ApiKey,
                        endpoint: d.HuggingFaceSettings?.Endpoint,
                        model: d.HuggingFaceSettings?.Model,
                        embeddingsMaxConcurrentBatches: d.HuggingFaceSettings?.EmbeddingsMaxConcurrentBatches,
                    },
                    ollamaSettings: {
                        model: d.OllamaSettings?.Model,
                        uri: d.OllamaSettings?.Uri,
                        think: d.OllamaSettings?.Think,
                        embeddingsMaxConcurrentBatches: d.OllamaSettings?.EmbeddingsMaxConcurrentBatches,
                        isSetTemperature: d.OllamaSettings?.Temperature != null,
                        temperature: d.OllamaSettings?.Temperature ?? null,
                    },
                    embeddedSettings: {
                        embeddingsMaxConcurrentBatches: d.EmbeddedSettings?.EmbeddingsMaxConcurrentBatches,
                    },
                    openAiSettings: {
                        apiKey: d.OpenAiSettings?.ApiKey,
                        endpoint: d.OpenAiSettings?.Endpoint,
                        model: d.OpenAiSettings?.Model,
                        organizationId: d.OpenAiSettings?.OrganizationId,
                        projectId: d.OpenAiSettings?.ProjectId,
                        dimensions: d.OpenAiSettings?.Dimensions,
                        embeddingsMaxConcurrentBatches: d.OpenAiSettings?.EmbeddingsMaxConcurrentBatches,
                        isSetTemperature: d.OpenAiSettings?.Temperature != null,
                        temperature: d.OpenAiSettings?.Temperature ?? null,
                    },
                    mistralAiSettings: {
                        apiKey: d.MistralAiSettings?.ApiKey,
                        endpoint: d.MistralAiSettings?.Endpoint,
                        model: d.MistralAiSettings?.Model,
                        embeddingsMaxConcurrentBatches: d.MistralAiSettings?.EmbeddingsMaxConcurrentBatches,
                    },
                    vertexSettings: {
                        aiVersion: d.VertexSettings?.AiVersion,
                        googleCredentialsJson: d.VertexSettings?.GoogleCredentialsJson,
                        location: d.VertexSettings?.Location,
                        model: d.VertexSettings?.Model,
                        embeddingsMaxConcurrentBatches: d.VertexSettings?.EmbeddingsMaxConcurrentBatches,
                    },
                } satisfies AiConnection);
                break;
            }
        }
    }

    return mapped;
}

export function mapAiConnectionsFromDto(
    connections: Record<string, AiConnectionStringDto>,
    ongoingTasks: OngoingTaskForConnection[]
): AiConnection[] {
    const type: AiConnection["type"] = "Ai";

    return Object.values(connections).map(
        (connection) =>
            ({
                type,
                name: connection.Name,
                usedByTasks: getConnectionStringUsedTasks(ongoingTasks, type, connection.Name),
                identifier: connection.Identifier,
                connectorType: getAiConnectorType(connection),
                modelType: connection.ModelType,
                azureOpenAiSettings: {
                    apiKey: connection.AzureOpenAiSettings?.ApiKey,
                    endpoint: connection.AzureOpenAiSettings?.Endpoint,
                    model: connection.AzureOpenAiSettings?.Model,
                    deploymentName: connection.AzureOpenAiSettings?.DeploymentName,
                    dimensions: connection.AzureOpenAiSettings?.Dimensions,
                    embeddingsMaxConcurrentBatches: connection.AzureOpenAiSettings?.EmbeddingsMaxConcurrentBatches,
                    enablePromptCache: connection.AzureOpenAiSettings?.EnablePromptCache ?? null,
                    isSetTemperature: connection.AzureOpenAiSettings?.Temperature != null,
                    temperature: connection.AzureOpenAiSettings?.Temperature ?? null,
                },
                googleSettings: {
                    aiVersion: connection.GoogleSettings?.AiVersion,
                    apiKey: connection.GoogleSettings?.ApiKey,
                    model: connection.GoogleSettings?.Model,
                    dimensions: connection.GoogleSettings?.Dimensions,
                    embeddingsMaxConcurrentBatches: connection.GoogleSettings?.EmbeddingsMaxConcurrentBatches,
                    enablePromptCache: connection.GoogleSettings?.EnablePromptCache ?? null,
                },
                huggingFaceSettings: {
                    apiKey: connection.HuggingFaceSettings?.ApiKey,
                    endpoint: connection.HuggingFaceSettings?.Endpoint,
                    model: connection.HuggingFaceSettings?.Model,
                    embeddingsMaxConcurrentBatches: connection.HuggingFaceSettings?.EmbeddingsMaxConcurrentBatches,
                },
                ollamaSettings: {
                    model: connection.OllamaSettings?.Model,
                    uri: connection.OllamaSettings?.Uri,
                    think: connection.OllamaSettings?.Think,
                    embeddingsMaxConcurrentBatches: connection.OllamaSettings?.EmbeddingsMaxConcurrentBatches,
                    isSetTemperature: connection.OllamaSettings?.Temperature != null,
                    temperature: connection.OllamaSettings?.Temperature ?? null,
                },
                embeddedSettings: {
                    embeddingsMaxConcurrentBatches: connection.EmbeddedSettings?.EmbeddingsMaxConcurrentBatches,
                },
                openAiSettings: {
                    apiKey: connection.OpenAiSettings?.ApiKey,
                    endpoint: connection.OpenAiSettings?.Endpoint,
                    model: connection.OpenAiSettings?.Model,
                    organizationId: connection.OpenAiSettings?.OrganizationId,
                    projectId: connection.OpenAiSettings?.ProjectId,
                    dimensions: connection.OpenAiSettings?.Dimensions,
                    embeddingsMaxConcurrentBatches: connection.OpenAiSettings?.EmbeddingsMaxConcurrentBatches,
                    enablePromptCache: connection.OpenAiSettings?.EnablePromptCache ?? null,
                    isSetTemperature: connection.OpenAiSettings?.Temperature != null,
                    temperature: connection.OpenAiSettings?.Temperature ?? null,
                },
                mistralAiSettings: {
                    apiKey: connection.MistralAiSettings?.ApiKey,
                    endpoint: connection.MistralAiSettings?.Endpoint,
                    model: connection.MistralAiSettings?.Model,
                    embeddingsMaxConcurrentBatches: connection.MistralAiSettings?.EmbeddingsMaxConcurrentBatches,
                },
                vertexSettings: {
                    aiVersion: connection.VertexSettings?.AiVersion,
                    googleCredentialsJson: connection.VertexSettings?.GoogleCredentialsJson,
                    location: connection.VertexSettings?.Location,
                    model: connection.VertexSettings?.Model,
                    embeddingsMaxConcurrentBatches: connection.VertexSettings?.EmbeddingsMaxConcurrentBatches,
                },
            }) satisfies AiConnection
    );
}
