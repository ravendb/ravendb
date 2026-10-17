using System;
using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.ConnectionStrings;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ConnectionStrings
{
    public abstract class RemoveConnectionStringCommand<T> : UpdateDatabaseCommand where T : ConnectionString
    {
        public string ConnectionStringName { get; protected set; }

        protected RemoveConnectionStringCommand()
        {
            // for deserialization
        }

        protected RemoveConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            ConnectionStringName = connectionStringName;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(ConnectionStringName)] = ConnectionStringName;
        }

        protected void Remove(DatabaseRecord record, ConnectionStringType type, Dictionary<string, T> connectionStrings)
        {
            if (ConnectionStringName.StartsWith(ServerWideConnectionString.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Can't remove connection string: '{ConnectionStringName}'. " +
                    $"Server-wide connection strings can only be removed via the server-wide connection strings API.");
            }

            var usage = ConnectionStringConsumers.FindUsage(record, type, ConnectionStringName);
            if (usage != null)
                throw new InvalidOperationException($"Can't delete connection string: {ConnectionStringName}. It is used by task: {usage.Value.Name}");

            connectionStrings.Remove(ConnectionStringName);
        }
    }

    public sealed class RemoveRavenConnectionStringCommand : RemoveConnectionStringCommand<RavenConnectionString>
    {
        public RemoveRavenConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveRavenConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Raven, record.RavenConnectionStrings);
        }
    }

    public sealed class RemoveSqlConnectionStringCommand : RemoveConnectionStringCommand<SqlConnectionString>
    {
        public RemoveSqlConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveSqlConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Sql, record.SqlConnectionStrings);
        }
    }

    public sealed class RemoveElasticSearchConnectionStringCommand : RemoveConnectionStringCommand<ElasticSearchConnectionString>
    {
        public RemoveElasticSearchConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveElasticSearchConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.ElasticSearch, record.ElasticSearchConnectionStrings);
        }
    }

    public sealed class RemoveOlapConnectionStringCommand : RemoveConnectionStringCommand<OlapConnectionString>
    {
        public RemoveOlapConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveOlapConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Olap, record.OlapConnectionStrings);
        }
    }

    public sealed class RemoveQueueConnectionStringCommand : RemoveConnectionStringCommand<QueueConnectionString>
    {
        public RemoveQueueConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveQueueConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Queue, record.QueueConnectionStrings);
        }
    }
    
    public sealed class RemoveSnowflakeConnectionStringCommand : RemoveConnectionStringCommand<SnowflakeConnectionString>
    {
        public RemoveSnowflakeConnectionStringCommand()
        {
            // for deserialization
        }

        public RemoveSnowflakeConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Snowflake, record.SnowflakeConnectionStrings);
        }
    }

    public sealed class RemoveAiConnectionStringCommand : RemoveConnectionStringCommand<AiConnectionString>
    {
        public RemoveAiConnectionStringCommand()
        {
            // for deserialization
        }
        public RemoveAiConnectionStringCommand(string connectionStringName, string databaseName, string uniqueRequestId) : base(connectionStringName, databaseName, uniqueRequestId)
        {
        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            Remove(record, ConnectionStringType.Ai, record.AiConnectionStrings);
        }
    }
}

