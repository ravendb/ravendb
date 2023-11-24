import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import SqlConnectionStringDto = Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString;

export interface ConnectionStringUsedTask {
    id: number;
    name: string;
}

type ConnectionCreator<
    Type extends StudioEtlType,
    Dto extends Raven.Client.Documents.Operations.ConnectionStrings.ConnectionString
> = Partial<Omit<Dto, "Type">> & { Type: Type } & {
    UsedByTasks?: ConnectionStringUsedTask[];
};

export type RavenDbConnection = ConnectionCreator<"Raven", RavenConnectionStringDto>;
export type SqlConnection = ConnectionCreator<"Sql", SqlConnectionStringDto>;
export type OlapConnection = ConnectionCreator<"Olap", OlapConnectionStringDto>;
export type ElasticSearchConnection = ConnectionCreator<"ElasticSearch", ElasticSearchConnectionStringDto>;
export type KafkaConnection = ConnectionCreator<"Kafka", QueueConnectionStringDto>;
export type RabbitMqConnection = ConnectionCreator<"RabbitMQ", QueueConnectionStringDto>;

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
