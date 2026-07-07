import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import AzureQueueStorageConnectionSettingsDto = Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings;
import AmazonSqsConnectionSettingsDto = Raven.Client.Documents.Operations.ETL.Queue.AmazonSqsConnectionSettings;
import AiConnectionSettingsDto = Raven.Client.Documents.Operations.AI.AiConnectionString;
import { FormDestinations } from "components/common/formDestinations/utils/formDestinationsTypes";

type SqlConnectionStringDto = SqlConnectionString;
type SnowflakeConnectionStringDto = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString;

export type StudioConnectionType =
    | "Raven"
    | "Sql"
    | "Snowflake"
    | "Olap"
    | "ElasticSearch"
    | "Kafka"
    | "RabbitMQ"
    | "AzureQueueStorage"
    | "AmazonSqs"
    | "AzureServiceBus"
    | "Ai";

export interface ConnectionStringUsage {
    kind: Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringUsageKind;
    id?: number;
    identifier?: string;
    name: string;
    // Set only for server-wide connection strings, whose usages are aggregated across databases.
    databaseName?: string;
}

interface ConnectionBase {
    name?: string;
    usedBy?: ConnectionStringUsage[];
    excludedDatabases?: string[];
}

export interface RavenConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Raven">;
    database?: string;
    topologyDiscoveryUrls?: {
        url: string;
    }[];
}

export interface SqlConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Sql">;
    connectionString?: string;
    factoryName?: SqlConnectionStringFactoryName;
}

export interface SnowflakeConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Snowflake">;
    connectionString?: string;
}

export interface OlapConnection extends ConnectionBase, FormDestinations {
    type: Extract<StudioConnectionType, "Olap">;
}

export type ElasticSearchAuthenticationMethod =
    | "No authentication"
    | "Basic"
    | "API Key"
    | "Encoded API Key"
    | "Certificate";

export interface ElasticSearchConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "ElasticSearch">;
    authMethodUsed?: ElasticSearchAuthenticationMethod;
    apiKey?: string;
    apiKeyId?: string;
    encodedApiKey?: string;
    password?: string;
    username?: string;
    certificatesBase64?: string[];
    nodes?: {
        url?: string;
    }[];
}

export interface KafkaConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Kafka">;
    bootstrapServers?: string;
    connectionOptions?: { key?: string; value?: string }[];
    isUseRavenCertificate?: boolean;
}

export interface RabbitMqConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "RabbitMQ">;
    connectionString?: string;
}

export interface AzureQueueStorageConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "AzureQueueStorage">;
    authType?: AzureQueueStorageAuthenticationType;
    settings?: {
        connectionString?: {
            connectionStringValue?: string;
        };
        entraId?: {
            clientId?: string;
            clientSecret?: string;
            storageAccountName?: string;
            tenantId?: string;
        };
        passwordless?: {
            storageAccountName?: string;
        };
    };
}

export interface AmazonSqsConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "AmazonSqs">;
    authType?: AmazonSqsAuthenticationType;
    settings?: {
        basic?: {
            accessKey?: string;
            regionName?: string;
            secretKey?: string;
        };
        passwordless?: boolean;
    };
}

export interface AzureServiceBusConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "AzureServiceBus">;
    authType?: AzureServiceBusAuthenticationType;
    settings?: {
        connectionString?: {
            connectionStringValue?: string;
        };
        entraId?: {
            namespace?: string;
            tenantId?: string;
            clientId?: string;
            clientSecret?: string;
        };
        passwordless?: {
            namespace?: string;
        };
    };
}

export interface AiConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Ai">;
    identifier?: string;
    connectorType?:
        | "azureOpenAiSettings"
        | "googleSettings"
        | "huggingFaceSettings"
        | "ollamaSettings"
        | "embeddedSettings"
        | "openAiSettings"
        | "mistralAiSettings"
        | "vertexSettings";
    modelType?: Raven.Client.Documents.Operations.AI.AiModelType;
    azureOpenAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        deploymentName?: string;
        dimensions?: number;
        embeddingsMaxConcurrentBatches?: number;
        enablePromptCache?: boolean;
        isSetTemperature?: boolean;
        temperature?: number;
    };
    googleSettings?: {
        aiVersion?: Raven.Client.Documents.Operations.AI.GoogleAIVersion;
        apiKey?: string;
        model?: string;
        dimensions?: number;
        endpoint?: string;
        embeddingsMaxConcurrentBatches?: number;
        enablePromptCache?: boolean;
    };
    huggingFaceSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        embeddingsMaxConcurrentBatches?: number;
    };
    ollamaSettings?: {
        model?: string;
        uri?: string;
        think?: boolean;
        embeddingsMaxConcurrentBatches?: number;
        isSetTemperature?: boolean;
        temperature?: number;
    };
    embeddedSettings?: {
        embeddingsMaxConcurrentBatches?: number;
    };
    openAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        organizationId?: string;
        projectId?: string;
        dimensions?: number;
        embeddingsMaxConcurrentBatches?: number;
        enablePromptCache?: boolean;
        isSetTemperature?: boolean;
        temperature?: number;
    };
    mistralAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        embeddingsMaxConcurrentBatches?: number;
    };
    vertexSettings?: {
        aiVersion?: Raven.Client.Documents.Operations.AI.VertexAIVersion;
        googleCredentialsJson?: string;
        location?: string;
        model?: string;
        embeddingsMaxConcurrentBatches?: number;
    };
}

export type Connection =
    | RavenConnection
    | SqlConnection
    | SnowflakeConnection
    | OlapConnection
    | ElasticSearchConnection
    | KafkaConnection
    | RabbitMqConnection
    | AzureQueueStorageConnection
    | AmazonSqsConnection
    | AzureServiceBusConnection
    | AiConnection;

export type ConnectionStringDto = Partial<
    | ElasticSearchConnectionStringDto
    | OlapConnectionStringDto
    | QueueConnectionStringDto
    | RavenConnectionStringDto
    | SqlConnectionStringDto
    | SnowflakeConnectionStringDto
    | AzureQueueStorageConnectionSettingsDto
    | AmazonSqsConnectionSettingsDto
    | AiConnectionSettingsDto
>;

export interface EditConnectionStringFormProps {
    initialConnection: Connection;
    isForNewConnection: boolean;
    onSave: (x: Connection) => void;
}

export type ConnectionFormData<T extends Connection> = Omit<T, "type" | "usedBy">;

export type WithExcludedDatabases<T> = T & {
    ExcludedDatabases?: string[];
};

export type ServerWideConnectionStringDto =
    | WithExcludedDatabases<RavenConnectionStringDto>
    | WithExcludedDatabases<SqlConnectionStringDto>
    | WithExcludedDatabases<SnowflakeConnectionStringDto>
    | WithExcludedDatabases<OlapConnectionStringDto>
    | WithExcludedDatabases<ElasticSearchConnectionStringDto>
    | WithExcludedDatabases<QueueConnectionStringDto>
    | WithExcludedDatabases<AiConnectionSettingsDto>;
