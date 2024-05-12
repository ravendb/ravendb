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
//TODO: map azure

export function mapAzureQueueStorageConnectionStringSettingsToDto(
    connection: Omit<AzureQueueStorageConnection, "type" | "usedByTasks">
): Raven.Client.Documents.Operations.ETL.Queue.AzureQueueStorageConnectionSettings {
    switch (connection.authType) {
        case "connectionString": {
            const connectionSettings = connection.settings[connection.authType];
            return {
                ConnectionString: connectionSettings.connectionStringValue,
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

export function mapConnectionStringToDto(connection: Connection): ConnectionStringDto {
    const type = connection.type;

    switch (type) {
        case "Raven":
            return mapRavenConnectionStringToDto(connection);
        case "Sql":
            return mapSqlConnectionStringToDto(connection);
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
        default:
            return assertUnreachable(type);
    }
}
