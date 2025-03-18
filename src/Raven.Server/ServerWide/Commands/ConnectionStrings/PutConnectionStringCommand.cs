using System;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class PutConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public T ConnectionString { get; protected set; }

        protected PutConnectionStringCommand()
        {
            // for deserialization
        }

        protected PutConnectionStringCommand(T connectionString, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ConnectionString = connectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionString)] = ConnectionString.ToJson();
        }
    }

    public sealed class PutRavenConnectionStringCommand : PutConnectionStringCommand<RavenConnectionString>
    {
        public PutRavenConnectionStringCommand()
        {
            // for deserialization
        }

        public PutRavenConnectionStringCommand(RavenConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            if (ConnectionString.Name.StartsWith(ServerWideExternalReplication.RavenConnectionStringPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var isNewConnectionString = record.RavenConnectionStrings.ContainsKey(ConnectionString.Name);
                throw new InvalidOperationException($"Can't {(isNewConnectionString ? "create" : "update")} connection string: '{ConnectionString.Name}'. " +
                                                          $"A regular (non server-wide) connection string name can't start with prefix '{ServerWideExternalReplication.RavenConnectionStringPrefix}'");
            }

            record.RavenConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }

    public sealed class PutSqlConnectionStringCommand : PutConnectionStringCommand<SqlConnectionString>
    {
        public PutSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public PutSqlConnectionStringCommand(SqlConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SqlConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }

    public sealed class PutOlapConnectionStringCommand : PutConnectionStringCommand<OlapConnectionString>
    {
        public PutOlapConnectionStringCommand()
        {
            // for deserialization
        }


        public PutOlapConnectionStringCommand(OlapConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.OlapConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }

    public sealed class PutElasticSearchConnectionStringCommand : PutConnectionStringCommand<ElasticSearchConnectionString>
    {
        public PutElasticSearchConnectionStringCommand()
        {
            // for deserialization
        }

        public PutElasticSearchConnectionStringCommand(ElasticSearchConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.ElasticSearchConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }

    public sealed class PutQueueConnectionStringCommand : PutConnectionStringCommand<QueueConnectionString>
    {
        public PutQueueConnectionStringCommand()
        {
            // for deserialization
        }

        public PutQueueConnectionStringCommand(QueueConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.QueueConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }
    
    public sealed class PutSnowflakeConnectionStringCommand : PutConnectionStringCommand<SnowflakeConnectionString>
    {
        public PutSnowflakeConnectionStringCommand()
        {
            // for deserialization
        }

        public PutSnowflakeConnectionStringCommand(SnowflakeConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.SnowflakeConnectionStrings[ConnectionString.Name] = ConnectionString;
        }
    }
}
