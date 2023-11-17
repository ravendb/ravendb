import ElasticSearchConnectionStringDto = Raven.Client.Documents.Operations.ETL.ElasticSearch.ElasticSearchConnectionString;
import OlapConnectionStringDto = Raven.Client.Documents.Operations.ETL.OLAP.OlapConnectionString;
import QueueConnectionStringDto = Raven.Client.Documents.Operations.ETL.Queue.QueueConnectionString;
import RavenConnectionStringDto = Raven.Client.Documents.Operations.ETL.RavenConnectionString;
import SqlConnectionStringDto = Raven.Client.Documents.Operations.ETL.SQL.SqlConnectionString;
import ConnectionStringTypeDto = Raven.Client.Documents.Operations.ConnectionStrings.ConnectionStringType;

type WithoutType<T> = Partial<Omit<T, "Type">>;

export interface ConnectionStringsUsedTask {
    id: number;
    name: string;
}

interface ConnectionBase {
    UsedByTasks?: ConnectionStringsUsedTask[];
}

export interface RavenDBConnection extends ConnectionBase, WithoutType<RavenConnectionStringDto> {
    Type: Extract<ConnectionStringTypeDto, "Raven">;
}

export interface SQLConnection extends ConnectionBase, WithoutType<SqlConnectionStringDto> {
    Type: Extract<ConnectionStringTypeDto, "Sql">;
}

export type Connection = RavenDBConnection;

export type ConnectionStringDto = Partial<
    | ElasticSearchConnectionStringDto
    | OlapConnectionStringDto
    | QueueConnectionStringDto
    | RavenConnectionStringDto
    | SqlConnectionStringDto
>;
