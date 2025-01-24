using System;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
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
        public T EtlConnectionString { get; protected set; }

        protected PutConnectionStringCommand()
        {
            // for deserialization
        }

        protected PutConnectionStringCommand(T etlConnectionString, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            EtlConnectionString = etlConnectionString;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(EtlConnectionString)] = EtlConnectionString.ToJson();
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
            if (EtlConnectionString.Name.StartsWith(ServerWideExternalReplication.RavenConnectionStringPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var isNewConnectionString = record.RavenConnectionStrings.ContainsKey(EtlConnectionString.Name);
                throw new InvalidOperationException($"Can't {(isNewConnectionString ? "create" : "update")} connection string: '{EtlConnectionString.Name}'. " +
                                                          $"A regular (non server-wide) connection string name can't start with prefix '{ServerWideExternalReplication.RavenConnectionStringPrefix}'");
            }

            record.RavenConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
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
            record.SqlConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
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
            record.OlapConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
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
            record.ElasticSearchConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
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
            record.QueueConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
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
            record.SnowflakeConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
        }
    }

    public sealed class PutAiConnectionStringCommand : PutConnectionStringCommand<AiEtlConnectionString>
    {
        public PutAiConnectionStringCommand()
        {
            // for deserialization
        }

        public PutAiConnectionStringCommand(AiEtlConnectionString etlConnectionString, string databaseName, string uniqueRequestId) : base(etlConnectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            record.AiConnectionStrings[EtlConnectionString.Name] = EtlConnectionString;
        }
    }
}
