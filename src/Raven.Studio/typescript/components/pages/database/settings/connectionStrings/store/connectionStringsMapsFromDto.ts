import {
    Connection,
    ConnectionStringUsage,
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

function mapRavenFromSingleDto(
    d: RavenConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): RavenConnection {
    return {
        type: "Raven",
        name: d.Name,
        database: d.Database,
        topologyDiscoveryUrls: d.TopologyDiscoveryUrls.map((url) => ({ url })),
        usedBy,
        excludedDatabases,
    } satisfies RavenConnection;
}

function mapSqlFromSingleDto(
    d: SqlConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): SqlConnection {
    return {
        type: "Sql",
        name: d.Name,
        connectionString: d.ConnectionString,
        factoryName: d.FactoryName,
        usedBy,
        excludedDatabases,
    } satisfies SqlConnection;
}

function mapSnowflakeFromSingleDto(
    d: SnowflakeConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): SnowflakeConnection {
    return {
        type: "Snowflake",
        name: d.Name,
        connectionString: d.ConnectionString,
        usedBy,
        excludedDatabases,
    } satisfies SnowflakeConnection;
}

export function mapRavenConnectionsFromDto(connections: Record<string, RavenConnectionStringDto>): RavenConnection[] {
    return Object.values(connections).map((d) =>
        mapRavenFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
    );
}

export function mapSqlConnectionsFromDto(connections: Record<string, SqlConnectionStringDto>): SqlConnection[] {
    return Object.values(connections).map((d) =>
        mapSqlFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
    );
}

export function mapSnowflakeConnectionsFromDto(
    connections: Record<string, SnowflakeConnectionStringDto>
): SnowflakeConnection[] {
    return Object.values(connections).map((d) =>
        mapSnowflakeFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
    );
}

function mapOlapFromSingleDto(
    d: OlapConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): OlapConnection {
    return {
        type: "Olap",
        name: d.Name,
        usedBy,
        excludedDatabases,
        ...mapDestinationsFromDto(_.omit(d, "Type", "Name")),
    } satisfies OlapConnection;
}

export function mapOlapConnectionsFromDto(connections: Record<string, OlapConnectionStringDto>): OlapConnection[] {
    return Object.values(connections).map((d) =>
        mapOlapFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
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

function mapElasticSearchFromSingleDto(
    d: ElasticSearchConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): ElasticSearchConnection {
    return {
        type: "ElasticSearch",
        name: d.Name,
        authMethodUsed: getElasticSearchAuthenticationMethod(d),
        apiKey: d.Authentication?.ApiKey?.ApiKey,
        apiKeyId: d.Authentication?.ApiKey?.ApiKeyId,
        username: d.Authentication?.Basic?.Username,
        password: d.Authentication?.Basic?.Password,
        certificatesBase64: d.Authentication?.Certificate?.CertificatesBase64,
        nodes: d.Nodes.map((url) => ({ url })),
        usedBy,
        excludedDatabases,
    } satisfies ElasticSearchConnection;
}

export function mapElasticSearchConnectionsFromDto(
    connections: Record<string, ElasticSearchConnectionStringDto>
): ElasticSearchConnection[] {
    return Object.values(connections).map((d) =>
        mapElasticSearchFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
    );
}

function mapKafkaFromSingleDto(
    d: QueueConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): KafkaConnection {
    return {
        type: "Kafka",
        name: d.Name,
        bootstrapServers: d.KafkaConnectionSettings.BootstrapServers,
        connectionOptions: Object.keys(d.KafkaConnectionSettings.ConnectionOptions).map((key) => ({
            key,
            value: d.KafkaConnectionSettings.ConnectionOptions[key],
        })),
        isUseRavenCertificate: d.KafkaConnectionSettings.UseRavenCertificate,
        usedBy,
        excludedDatabases,
    } satisfies KafkaConnection;
}

function mapRabbitMqFromSingleDto(
    d: QueueConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): RabbitMqConnection {
    return {
        type: "RabbitMQ",
        name: d.Name,
        connectionString: d.RabbitMqConnectionSettings.ConnectionString,
        usedBy,
        excludedDatabases,
    } satisfies RabbitMqConnection;
}

export function mapKafkaConnectionsFromDto(connections: Record<string, QueueConnectionStringDto>): KafkaConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "Kafka")
        .map((d) =>
            mapKafkaFromSingleDto(
                d,
                (d.UsedBy ?? []).map(
                    (t) =>
                        ({
                            kind: t.Kind,
                            id: t.Id,
                            identifier: t.Identifier,
                            name: t.Name,
                        }) satisfies ConnectionStringUsage
                )
            )
        );
}

export function mapRabbitMqConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>
): RabbitMqConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "RabbitMq")
        .map((d) =>
            mapRabbitMqFromSingleDto(
                d,
                (d.UsedBy ?? []).map(
                    (t) =>
                        ({
                            kind: t.Kind,
                            id: t.Id,
                            identifier: t.Identifier,
                            name: t.Name,
                        }) satisfies ConnectionStringUsage
                )
            )
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

function mapAzureQueueStorageFromSingleDto(
    d: QueueConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): AzureQueueStorageConnection {
    return {
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
        usedBy,
        excludedDatabases,
    } satisfies AzureQueueStorageConnection;
}

function mapAmazonSqsFromSingleDto(
    d: QueueConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): AmazonSqsConnection {
    return {
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
        usedBy,
        excludedDatabases,
    } satisfies AmazonSqsConnection;
}

export function mapAzureQueueStorageConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>
): AzureQueueStorageConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "AzureQueueStorage")
        .map((d) =>
            mapAzureQueueStorageFromSingleDto(
                d,
                (d.UsedBy ?? []).map(
                    (t) =>
                        ({
                            kind: t.Kind,
                            id: t.Id,
                            identifier: t.Identifier,
                            name: t.Name,
                        }) satisfies ConnectionStringUsage
                )
            )
        );
}

export function mapAmazonSqsConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>
): AmazonSqsConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "AmazonSqs")
        .map((d) =>
            mapAmazonSqsFromSingleDto(
                d,
                (d.UsedBy ?? []).map(
                    (t) =>
                        ({
                            kind: t.Kind,
                            id: t.Id,
                            identifier: t.Identifier,
                            name: t.Name,
                        }) satisfies ConnectionStringUsage
                )
            )
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

function mapAzureServiceBusFromSingleDto(
    d: QueueConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): AzureServiceBusConnection {
    return {
        type: "AzureServiceBus",
        name: d.Name,
        authType: getAzureServiceBusAuthType(d),
        settings: {
            connectionString: {
                connectionStringValue: d.AzureServiceBusConnectionSettings.ConnectionString,
            },
            entraId: {
                namespace: d.AzureServiceBusConnectionSettings.EntraId?.Namespace,
                tenantId: d.AzureServiceBusConnectionSettings.EntraId?.TenantId,
                clientId: d.AzureServiceBusConnectionSettings.EntraId?.ClientId,
                clientSecret: d.AzureServiceBusConnectionSettings.EntraId?.ClientSecret,
            },
            passwordless: {
                namespace: d.AzureServiceBusConnectionSettings.Passwordless?.Namespace,
            },
        },
        usedBy,
        excludedDatabases,
    } satisfies AzureServiceBusConnection;
}

export function mapAzureServiceBusConnectionsFromDto(
    connections: Record<string, QueueConnectionStringDto>
): AzureServiceBusConnection[] {
    return Object.values(connections)
        .filter((x) => x.BrokerType === "AzureServiceBus")
        .map((d) =>
            mapAzureServiceBusFromSingleDto(
                d,
                (d.UsedBy ?? []).map(
                    (t) =>
                        ({
                            kind: t.Kind,
                            id: t.Id,
                            identifier: t.Identifier,
                            name: t.Name,
                        }) satisfies ConnectionStringUsage
                )
            )
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

function mapAiFromSingleDto(
    d: AiConnectionStringDto,
    usedBy: ConnectionStringUsage[],
    excludedDatabases?: string[]
): AiConnection {
    return {
        type: "Ai",
        name: d.Name,
        usedBy,
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
            enablePromptCache: d.AzureOpenAiSettings?.EnablePromptCache,
            isSetTemperature: d.AzureOpenAiSettings?.Temperature != null,
            temperature: d.AzureOpenAiSettings?.Temperature ?? null,
        },
        googleSettings: {
            aiVersion: d.GoogleSettings?.AiVersion,
            apiKey: d.GoogleSettings?.ApiKey,
            model: d.GoogleSettings?.Model,
            dimensions: d.GoogleSettings?.Dimensions,
            embeddingsMaxConcurrentBatches: d.GoogleSettings?.EmbeddingsMaxConcurrentBatches,
            enablePromptCache: d.GoogleSettings?.EnablePromptCache,
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
            enablePromptCache: d.OpenAiSettings?.EnablePromptCache,
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
    } satisfies AiConnection;
}

export function mapAiConnectionsFromDto(connections: Record<string, AiConnectionStringDto>): AiConnection[] {
    return Object.values(connections).map((d) =>
        mapAiFromSingleDto(
            d,
            (d.UsedBy ?? []).map(
                (t) =>
                    ({ kind: t.Kind, id: t.Id, identifier: t.Identifier, name: t.Name }) satisfies ConnectionStringUsage
            )
        )
    );
}

export function mapAllConnectionsFromDto(connectionStringsDto: GetConnectionStringsResult): {
    [key in StudioConnectionType]: Connection[];
} {
    return {
        Raven: mapRavenConnectionsFromDto(connectionStringsDto.RavenConnectionStrings),
        Sql: mapSqlConnectionsFromDto(connectionStringsDto.SqlConnectionStrings),
        Snowflake: mapSnowflakeConnectionsFromDto(connectionStringsDto.SnowflakeConnectionStrings),
        Olap: mapOlapConnectionsFromDto(connectionStringsDto.OlapConnectionStrings),
        ElasticSearch: mapElasticSearchConnectionsFromDto(connectionStringsDto.ElasticSearchConnectionStrings),
        Kafka: mapKafkaConnectionsFromDto(connectionStringsDto.QueueConnectionStrings),
        RabbitMQ: mapRabbitMqConnectionsFromDto(connectionStringsDto.QueueConnectionStrings),
        AzureQueueStorage: mapAzureQueueStorageConnectionsFromDto(connectionStringsDto.QueueConnectionStrings),
        AmazonSqs: mapAmazonSqsConnectionsFromDto(connectionStringsDto.QueueConnectionStrings),
        AzureServiceBus: mapAzureServiceBusConnectionsFromDto(connectionStringsDto.QueueConnectionStrings),
        Ai: mapAiConnectionsFromDto(connectionStringsDto.AiConnectionStrings),
    };
}

type WithExcludedDatabases<T> = T & {
    ExcludedDatabases?: string[];
};

export type ServerWideConnectionStringDto =
    | WithExcludedDatabases<RavenConnectionStringDto>
    | WithExcludedDatabases<SqlConnectionStringDto>
    | WithExcludedDatabases<SnowflakeConnectionStringDto>
    | WithExcludedDatabases<OlapConnectionStringDto>
    | WithExcludedDatabases<ElasticSearchConnectionStringDto>
    | WithExcludedDatabases<QueueConnectionStringDto>
    | WithExcludedDatabases<AiConnectionStringDto>;

export function mapServerWideConnectionsFromDto(results: ServerWideConnectionStringDto[]): {
    [key in StudioConnectionType]: Connection[];
} {
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
        AzureServiceBus: [],
        Ai: [],
    };

    for (const dto of results) {
        const excludedDatabases = dto.ExcludedDatabases ?? [];
        const usedBy = (dto.UsedBy ?? []).map(
            (t) =>
                ({
                    kind: t.Kind,
                    id: t.Id,
                    identifier: t.Identifier,
                    name: t.Name,
                    databaseName: t.DatabaseName,
                }) satisfies ConnectionStringUsage
        );
        switch (dto.Type) {
            case "Raven": {
                const d = dto as WithExcludedDatabases<RavenConnectionStringDto>;
                mapped.Raven.push(mapRavenFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "Sql": {
                const d = dto as WithExcludedDatabases<SqlConnectionStringDto>;
                mapped.Sql.push(mapSqlFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "Snowflake": {
                const d = dto as WithExcludedDatabases<SnowflakeConnectionStringDto>;
                mapped.Snowflake.push(mapSnowflakeFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "Olap": {
                const d = dto as WithExcludedDatabases<OlapConnectionStringDto>;
                mapped.Olap.push(mapOlapFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "ElasticSearch": {
                const d = dto as WithExcludedDatabases<ElasticSearchConnectionStringDto>;
                mapped.ElasticSearch.push(mapElasticSearchFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "Queue": {
                const d = dto as WithExcludedDatabases<QueueConnectionStringDto>;
                switch (d.BrokerType) {
                    case "Kafka":
                        mapped.Kafka.push(mapKafkaFromSingleDto(d, usedBy, excludedDatabases));
                        break;
                    case "RabbitMq":
                        mapped.RabbitMQ.push(mapRabbitMqFromSingleDto(d, usedBy, excludedDatabases));
                        break;
                    case "AzureQueueStorage":
                        mapped.AzureQueueStorage.push(mapAzureQueueStorageFromSingleDto(d, usedBy, excludedDatabases));
                        break;
                    case "AmazonSqs":
                        mapped.AmazonSqs.push(mapAmazonSqsFromSingleDto(d, usedBy, excludedDatabases));
                        break;
                    case "AzureServiceBus":
                        mapped.AzureServiceBus.push(mapAzureServiceBusFromSingleDto(d, usedBy, excludedDatabases));
                        break;
                    case "None":
                        break;
                    default:
                        assertUnreachable(d.BrokerType);
                }
                break;
            }
            case "Ai": {
                const d = dto as WithExcludedDatabases<AiConnectionStringDto>;
                mapped.Ai.push(mapAiFromSingleDto(d, usedBy, excludedDatabases));
                break;
            }
            case "None":
                break;
            default:
                assertUnreachable(dto.Type);
        }
    }

    return mapped;
}
