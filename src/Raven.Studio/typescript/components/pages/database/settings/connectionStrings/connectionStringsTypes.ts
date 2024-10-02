import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import AzureQueueStorageConnectionSettingsDto = Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings;

type SqlConnectionStringDto = SqlConnectionString;
type SnowflakeConnectionStringDto = Raven.Client.Documents.Operations.ETL.Snowflake.SnowflakeConnectionString;
import { FormDestinations } from "components/common/formDestinations/utils/formDestinationsTypes";

export interface ConnectionStringUsedTask {
    id: number;
    name: string;
}

interface ConnectionBase {
    name?: string;
    usedByTasks?: ConnectionStringUsedTask[];
}

export interface RavenConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "Raven">;
    database?: string;
    topologyDiscoveryUrls?: {
        url: string;
    }[];
}

export interface SqlConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "Sql">;
    connectionString?: string;
    factoryName?: SqlConnectionStringFactoryName;
}

export interface SnowflakeConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "Snowflake">;
    connectionString?: string;
}

export interface OlapConnection extends ConnectionBase, FormDestinations {
    type: Extract<StudioEtlType, "Olap">;
}

export type ElasticSearchAuthenticationMethod =
    | "No authentication"
    | "Basic"
    | "API Key"
    | "Encoded API Key"
    | "Certificate";

export interface ElasticSearchConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "ElasticSearch">;
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
    type: Extract<StudioEtlType, "Kafka">;
    bootstrapServers?: string;
    connectionOptions?: { key?: string; value?: string }[];
    isUseRavenCertificate?: boolean;
}

export interface RabbitMqConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "RabbitMQ">;
    connectionString?: string;
}

export interface AzureQueueStorageConnection extends ConnectionBase {
    type: Extract<StudioEtlType, "AzureQueueStorage">;
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

export type Connection =
    | RavenConnection
    | SqlConnection
    | SnowflakeConnection
    | OlapConnection
    | ElasticSearchConnection
    | KafkaConnection
    | RabbitMqConnection
    | AzureQueueStorageConnection;

export type ConnectionStringDto = Partial<
    | ElasticSearchConnectionStringDto
    | OlapConnectionStringDto
    | QueueConnectionStringDto
    | RavenConnectionStringDto
    | SqlConnectionStringDto
    | SnowflakeConnectionStringDto
    | AzureQueueStorageConnectionSettingsDto
>;

export interface EditConnectionStringFormProps {
    initialConnection: Connection;
    isForNewConnection: boolean;
    onSave: (x: Connection) => void;
}

export type ConnectionFormData<T extends Connection> = Omit<T, "type" | "usedByTasks">;
