import { mapDestinationsToDto } from "components/common/formDestinations/utils/formDestinationsMapsToDto";
import {
    Connection,
    ConnectionStringDto,
    RavenConnection,
    SqlConnection,
    ElasticSearchConnection,
    KafkaConnection,
    RabbitMqConnection,
    AzureQueueStorageConnection,
    OlapConnection,
    ConnectionFormData,
    SnowflakeConnection,
    AmazonSqsConnection,
    AiConnection,
} from "../connectionStringsTypes";
import assertUnreachable from "components/utils/assertUnreachable";
import ApiKeyAuthentication = Raven.Client.Documents.Operations.ETL.ElasticSearch.ApiKeyAuthentication;

export function mapRavenConnectionStringToDto(connection: RavenConnection): ConnectionStringDto {
    return {
        Type: "Raven",
        Name: connection.name,
        Database: connection.database,
        TopologyDiscoveryUrls: connection.topologyDiscoveryUrls.map((x) => x.url),
    };
}

export function mapSqlConnectionStringToDto(connection: SqlConnection): ConnectionStringDto {
    return {
        Type: "Sql",
        Name: connection.name,
        FactoryName: connection.factoryName,
        ConnectionString: connection.connectionString,
    };
}

export function mapSnowflakeConnectionStringToDto(connection: SnowflakeConnection): ConnectionStringDto {
    return {
        Type: "Snowflake",
        Name: connection.name,
        ConnectionString: connection.connectionString,
    };
}

export function mapOlapConnectionStringToDto(connection: OlapConnection): ConnectionStringDto {
    return {
        Type: "Olap",
        Name: connection.name,
        ...mapDestinationsToDto(connection.destinations),
    };
}

export function mapElasticSearchAuthenticationToDto(
    formValues: ConnectionFormData<ElasticSearchConnection>
): Raven.Client.Documents.Operations.ETL.ElasticSearch.Authentication {
    const auth = formValues.authMethodUsed;

    const apiKey: ApiKeyAuthentication =
        auth === "API Key" || auth === "Encoded API Key"
            ? {
                  ApiKey: formValues.authMethodUsed === "API Key" ? formValues.apiKey : undefined,
                  ApiKeyId: formValues.authMethodUsed === "API Key" ? formValues.apiKeyId : undefined,
                  EncodedApiKey: formValues.authMethodUsed === "Encoded API Key" ? formValues.encodedApiKey : undefined,
              }
            : undefined;

    return {
        ApiKey: apiKey,
        Basic:
            auth === "Basic"
                ? {
                      Username: formValues.username,
                      Password: formValues.password,
                  }
                : null,
        Certificate:
            auth === "Certificate"
                ? {
                      CertificatesBase64: formValues.certificatesBase64,
                  }
                : null,
    };
}

export function mapElasticSearchConnectionStringToDto(connection: ElasticSearchConnection): ConnectionStringDto {
    return {
        Type: "ElasticSearch",
        Name: connection.name,
        Nodes: connection.nodes.map((x) => x.url),
        Authentication: mapElasticSearchAuthenticationToDto(connection),
    };
}

export function mapKafkaConnectionStringToDto(connection: KafkaConnection): ConnectionStringDto {
    return {
        Type: "Queue",
        BrokerType: "Kafka",
        Name: connection.name,
        KafkaConnectionSettings: {
            BootstrapServers: connection.bootstrapServers,
            ConnectionOptions: Object.fromEntries(connection.connectionOptions.map((x) => [x.key, x.value])),
            UseRavenCertificate: connection.isUseRavenCertificate,
        },
    };
}

export function mapRabbitMqStringToDto(connection: RabbitMqConnection): ConnectionStringDto {
    return {
        Type: "Queue",
        BrokerType: "RabbitMq",
        Name: connection.name,
        RabbitMqConnectionSettings: {
            ConnectionString: connection.connectionString,
        },
    };
}

export function mapAzureQueueStorageConnectionStringSettingsToDto(
    connection: Omit<AzureQueueStorageConnection, "type" | "usedByTasks">
): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
    switch (connection.authType) {
        case "connectionString": {
            const connectionSettings = connection.settings[connection.authType];
            const connectionStringWithoutNewLines = connectionSettings.connectionStringValue.replace(/\n/g, "");

            return {
                ConnectionString: connectionStringWithoutNewLines,
                EntraId: null,
                Passwordless: null,
            };
        }
        case "entraId": {
            const connectionSettings = connection.settings[connection.authType];
            return {
                ConnectionString: null,
                EntraId: {
                    StorageAccountName: connectionSettings.storageAccountName,
                    TenantId: connectionSettings.tenantId,
                    ClientId: connectionSettings.clientId,
                    ClientSecret: connectionSettings.clientSecret,
                },
                Passwordless: null,
            };
        }
        case "passwordless": {
            const connectionSettings = connection.settings[connection.authType];
            return {
                ConnectionString: null,
                EntraId: null,
                Passwordless: {
                    StorageAccountName: connectionSettings.storageAccountName,
                },
            };
        }
        default:
            return assertUnreachable(connection.authType);
    }
}

export function mapAmazonSqsConnectionStringSettingsToDto(
    connection: Omit<AmazonSqsConnection, "type" | "usedByTasks">
): Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings {
    switch (connection.authType) {
        case "basic": {
            return {
                Basic: {
                    SecretKey: connection.settings.basic.secretKey,
                    AccessKey: connection.settings.basic.accessKey,
                    RegionName: connection.settings.basic.regionName,
                },
                Passwordless: false,
            };
        }
        case "passwordless": {
            return {
                Basic: null,
                Passwordless: true,
            };
        }
        default:
            return assertUnreachable(connection.authType);
    }
}

export function mapAzureQueueStorageConnectionStringToDto(
    connection: AzureQueueStorageConnection
): ConnectionStringDto {
    return {
        Type: "Queue",
        BrokerType: "AzureQueueStorage",
        Name: connection.name,
        AzureQueueStorageConnectionSettings: mapAzureQueueStorageConnectionStringSettingsToDto(connection),
    };
}

export function mapAmazonSqsConnectionStringToDto(connection: AmazonSqsConnection): ConnectionStringDto {
    return {
        Type: "Queue",
        BrokerType: "AmazonSqs",
        Name: connection.name,
        AmazonSqsConnectionSettings: mapAmazonSqsConnectionStringSettingsToDto(connection),
    };
}

export function mapAiConnectionStringToDto(connection: AiConnection): ConnectionStringDto {
    return {
        Type: "Ai",
        Name: connection.name,
        Identifier: connection.identifier,
        ModelType: connection.modelType,
        AzureOpenAiSettings:
            connection.connectorType === "azureOpenAiSettings"
                ? {
                      ApiKey: connection.azureOpenAiSettings.apiKey,
                      Endpoint: connection.azureOpenAiSettings.endpoint,
                      Model: connection.azureOpenAiSettings.model,
                      DeploymentName: connection.azureOpenAiSettings.deploymentName,
                      Dimensions: mapDimensionsToDto(connection),
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                      EnablePromptCache: connection.azureOpenAiSettings.enablePromptCache,
                      Temperature: mapTemperatureToDto(connection),
                  }
                : null,
        GoogleSettings:
            connection.connectorType === "googleSettings"
                ? {
                      ApiKey: connection.googleSettings.apiKey,
                      Model: connection.googleSettings.model,
                      AiVersion: connection.googleSettings.aiVersion,
                      Dimensions: mapDimensionsToDto(connection),
                      Endpoint: connection.googleSettings.endpoint,
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                      EnablePromptCache: connection.googleSettings.enablePromptCache,
                  }
                : null,
        HuggingFaceSettings:
            connection.connectorType === "huggingFaceSettings"
                ? {
                      ApiKey: connection.huggingFaceSettings.apiKey,
                      Endpoint: connection.huggingFaceSettings.endpoint,
                      Model: connection.huggingFaceSettings.model,
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                  }
                : null,
        OllamaSettings:
            connection.connectorType === "ollamaSettings"
                ? {
                      Model: connection.ollamaSettings.model,
                      Uri: connection.ollamaSettings.uri,
                      Endpoint: connection.ollamaSettings.uri,
                      ApiKey: null,
                      Think: connection.modelType === "Chat" ? connection.ollamaSettings.think : null,
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                      Temperature: mapTemperatureToDto(connection),
                  }
                : null,
        EmbeddedSettings:
            connection.connectorType === "embeddedSettings"
                ? {
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                  }
                : null,
        OpenAiSettings:
            connection.connectorType === "openAiSettings"
                ? {
                      ApiKey: connection.openAiSettings.apiKey,
                      Endpoint: connection.openAiSettings.endpoint,
                      Model: connection.openAiSettings.model,
                      OrganizationId: connection.openAiSettings.organizationId,
                      ProjectId: connection.openAiSettings.projectId,
                      Dimensions: mapDimensionsToDto(connection),
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                      EnablePromptCache: connection.openAiSettings.enablePromptCache,
                      Temperature: mapTemperatureToDto(connection),
                  }
                : null,
        MistralAiSettings:
            connection.connectorType === "mistralAiSettings"
                ? {
                      ApiKey: connection.mistralAiSettings.apiKey,
                      Endpoint: connection.mistralAiSettings.endpoint,
                      Model: connection.mistralAiSettings.model,
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                  }
                : null,
        VertexSettings:
            connection.connectorType === "vertexSettings"
                ? {
                      GoogleCredentialsJson: connection.vertexSettings.googleCredentialsJson,
                      Location: connection.vertexSettings.location,
                      Model: connection.vertexSettings.model,
                      AiVersion: connection.vertexSettings.aiVersion,
                      EmbeddingsMaxConcurrentBatches: mapEmbeddingsMaxConcurrentBatchesToDto(connection),
                  }
                : null,
    };
}

function mapEmbeddingsMaxConcurrentBatchesToDto(connection: AiConnection): number {
    if (connection.modelType !== "TextEmbeddings") {
        return null;
    }

    return connection[connection.connectorType].embeddingsMaxConcurrentBatches;
}

function mapDimensionsToDto(connection: AiConnection): number {
    const connectorType = connection.connectorType;

    if (
        connectorType !== "azureOpenAiSettings" &&
        connectorType !== "openAiSettings" &&
        connectorType !== "googleSettings"
    ) {
        return null;
    }

    if (connection.modelType !== "TextEmbeddings") {
        return null;
    }

    return connection[connectorType].dimensions;
}

function mapTemperatureToDto(connection: AiConnection): number {
    const connectorType = connection.connectorType;

    if (
        connectorType !== "openAiSettings" &&
        connectorType !== "ollamaSettings" &&
        connectorType !== "azureOpenAiSettings"
    ) {
        return null;
    }

    if (connection.modelType !== "Chat" || !connection[connectorType].isSetTemperature) {
        return null;
    }

    return connection[connectorType].temperature;
}

export function mapConnectionStringToDto(connection: Connection): ConnectionStringDto {
    const type = connection.type;

    switch (type) {
        case "Raven":
            return mapRavenConnectionStringToDto(connection);
        case "Sql":
            return mapSqlConnectionStringToDto(connection);
        case "Snowflake":
            return mapSnowflakeConnectionStringToDto(connection);
        case "Olap":
            return mapOlapConnectionStringToDto(connection);
        case "ElasticSearch":
            return mapElasticSearchConnectionStringToDto(connection);
        case "Kafka":
            return mapKafkaConnectionStringToDto(connection);
        case "RabbitMQ":
            return mapRabbitMqStringToDto(connection);
        case "AzureQueueStorage":
            return mapAzureQueueStorageConnectionStringToDto(connection);
        case "AmazonSqs":
            return mapAmazonSqsConnectionStringToDto(connection);
        case "Ai":
            return mapAiConnectionStringToDto(connection);
        default:
            return assertUnreachable(type);
    }
}
