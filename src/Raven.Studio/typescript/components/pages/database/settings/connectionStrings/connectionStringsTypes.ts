import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import SqlConnectionStringDto = Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString;

type WithoutType<T> = Partial<Omit<T, "Type">>;

export interface ConnectionStringsUsedTask {
    id: number;
    name: string;
}

interface ConnectionBase {
    UsedByTasks?: ConnectionStringsUsedTask[];
}

export interface RavenDbConnection extends ConnectionBase, WithoutType<RavenConnectionStringDto> {
    Type: Extract<StudioEtlType, "Raven">;
}

export interface SqlConnection extends ConnectionBase, WithoutType<SqlConnectionStringDto> {
    Type: Extract<StudioEtlType, "Sql">;
}

export interface OlapConnection extends ConnectionBase, WithoutType<OlapConnectionStringDto> {
    Type: Extract<StudioEtlType, "Olap">;
}

export interface ElasticSearchConnection extends ConnectionBase, WithoutType<ElasticSearchConnectionStringDto> {
    Type: Extract<StudioEtlType, "ElasticSearch">;
}

export interface KafkaConnection extends ConnectionBase, WithoutType<QueueConnectionStringDto> {
    Type: Extract<StudioEtlType, "Kafka">;
}

export interface RabbitMqConnection extends ConnectionBase, WithoutType<RavenConnectionStringDto> {
    Type: Extract<StudioEtlType, "RabbitMQ">;
}

export type Connection =
    | RavenDbConnection
    | SqlConnection
    | OlapConnection
    | ElasticSearchConnection
    | KafkaConnection
    | RabbitMqConnection;

export type ConnectionStringDto = Partial<
    | ElasticSearchConnectionStringDto
    | OlapConnectionStringDto
    | QueueConnectionStringDto
    | RavenConnectionStringDto
    | SqlConnectionStringDto
>;
