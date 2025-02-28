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
    | "Ai";

export interface ConnectionStringUsedTask {
    id: number;
    name: string;
}

interface ConnectionBase {
    name?: string;
    usedByTasks?: ConnectionStringUsedTask[];
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

export interface AiConnection extends ConnectionBase {
    type: Extract<StudioConnectionType, "Ai">;
    identifier?: string;
    connectorType?:
        | "azureOpenAiSettings"
        | "googleSettings"
        | "huggingFaceSettings"
        | "ollamaSettings"
        | "onnxSettings"
        | "openAiSettings"
        | "mistralaiAiSettings";
    azureOpenAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        deploymentName?: string;
        dimensions?: number;
    };
    googleSettings?: {
        aiVersion?: Raven.Client.Documents.Operations.AI.GoogleAIVersion;
        apiKey?: string;
        model?: string;
    };
    huggingFaceSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
    };
    ollamaSettings?: {
        model?: string;
        uri?: string;
    };
    onnxSettings?: {
        caseSensitive?: boolean;
        clsToken?: string;
        maximumTokens?: number;
        normalizeEmbeddings?: boolean;
        padToken?: string;
        poolingMode?: Raven.Client.Documents.Operations.AI.OnnxEmbeddingPoolingMode;
        sepToken?: string;
        unicodeNormalization?: System.Text.NormalizationForm;
        unknownToken?: string;
    };
    openAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
        organizationId?: string;
        projectId?: string;
    };
    mistralaiAiSettings?: {
        apiKey?: string;
        endpoint?: string;
        model?: string;
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

export type ConnectionFormData<T extends Connection> = Omit<T, "type" | "usedByTasks">;
