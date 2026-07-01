using System;
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

        protected static void AssertNotServerWideConnectionString(string name)
        {
            if (name.StartsWith(ServerWideConnectionString.NamePrefix, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException(
                    $"Can't remove connection string: '{name}'. " +
                    $"Server-wide connection strings can only be removed via the server-wide connection strings API.");
            }
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.RavenConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.SqlConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.ElasticSearchConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.OlapConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.QueueConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.SnowflakeConnectionStrings.Remove(ConnectionStringName);
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
            AssertNotServerWideConnectionString(ConnectionStringName);
            record.AiConnectionStrings.Remove(ConnectionStringName);
        }
    }
}

