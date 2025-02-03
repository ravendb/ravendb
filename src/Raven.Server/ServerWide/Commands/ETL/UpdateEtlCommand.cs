using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;

namespace Raven.Server.ServerWide.Commands.ETL
{
    public abstract class UpdateEtlCommand<T, TConnectionString> : UpdateDatabaseCommand where T : EtlConfiguration<TConnectionString> where TConnectionString : ConnectionString
    {
        public long TaskId { get; protected set; }

        public T Configuration { get; protected set; }

        public EtlType EtlType { get; protected set; }

        protected UpdateEtlCommand()
        {
            // for deserialization
        }

        protected UpdateEtlCommand(long taskId, T configuration, EtlType type, string databaseName, string uniqueRequestId) : base(databaseName, uniqueRequestId)
        {
            TaskId = taskId;
            Configuration = configuration;
            EtlType = type;
        }

        public override void FillJson(DynamicJsonValue json)
        {
            json[nameof(TaskId)] = TaskId;
            json[nameof(Configuration)] = TypeConverter.ToBlittableSupportedType(Configuration);
            json[nameof(EtlType)] = EtlType;
        }
    }

    public sealed class UpdateRavenEtlCommand : UpdateEtlCommand<RavenEtlConfiguration, RavenConnectionString>
    {
        public UpdateRavenEtlCommand()
        {
            // for deserialization
        }

        public UpdateRavenEtlCommand(long taskId, RavenEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Raven, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.RavenEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddRavenEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);

        }
    }

    public sealed class UpdateSqlEtlCommand : UpdateEtlCommand<SqlEtlConfiguration, SqlConnectionString>
    {
        public UpdateSqlEtlCommand()
        {
            // for deserialization
        }

        public UpdateSqlEtlCommand(long taskId, SqlEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Sql, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.SqlEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddSqlEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);

        }
    }

    public sealed class UpdateOlapEtlCommand : UpdateEtlCommand<OlapEtlConfiguration, OlapConnectionString>
    {
        public UpdateOlapEtlCommand()
        {
            // for deserialization
        }

        public UpdateOlapEtlCommand(long taskId, OlapEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Olap, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.OlapEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddOlapEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
    }
    
    public sealed class UpdateElasticSearchEtlCommand : UpdateEtlCommand<ElasticSearchEtlConfiguration, ElasticSearchConnectionString>
    {
        public UpdateElasticSearchEtlCommand()
        {
            // for deserialization
        }

        public UpdateElasticSearchEtlCommand(long taskId, ElasticSearchEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.ElasticSearch, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.ElasticSearchEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddElasticSearchEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
    }

    public sealed class UpdateQueueEtlCommand : UpdateEtlCommand<QueueEtlConfiguration, QueueConnectionString>
    {
        public UpdateQueueEtlCommand()
        {
            // for deserialization
        }

        public UpdateQueueEtlCommand(long taskId, QueueEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Queue, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.QueueEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddQueueEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
    }
    
    
    public sealed class UpdateSnowflakeEtlCommand : UpdateEtlCommand<SnowflakeEtlConfiguration, SnowflakeConnectionString>
    {
        public UpdateSnowflakeEtlCommand()
        {
            // for deserialization
        }

        public UpdateSnowflakeEtlCommand(long taskId, SnowflakeEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Snowflake, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.SnowflakeEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddSnowflakeEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }
    }

    public sealed class UpdateAiEtlCommand : UpdateEtlCommand<AiEtlConfiguration, AiConnectionString>
    {
        public UpdateAiEtlCommand()
        {
            // for deserialization
        }

        public UpdateAiEtlCommand(long taskId, AiEtlConfiguration configuration, string databaseName, string uniqueRequestId) : base(taskId, configuration, EtlType.Ai, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            InClusterValidation(record);

            new DeleteOngoingTaskCommand(TaskId, OngoingTaskType.AiEtl, DatabaseName, null).UpdateDatabaseRecord(record, etag);
            new AddAiEtlCommand(Configuration, DatabaseName, null).UpdateDatabaseRecord(record, etag);
        }

        private void InClusterValidation(DatabaseRecord record)
        {
            try
            {
                if (record == null)
                    throw new RachisApplyException("Failed to get database record, but it is required for further validation");

                var oldConfig = record.AiEtls.FirstOrDefault(x => x.Name == Configuration.Name);
                if (oldConfig == null)
                    return;

                if (oldConfig.AiConnectorType != Configuration.AiConnectorType)
                {
                    throw new RachisApplyException(
                        $"Cannot update AI ETL task '{Configuration.Name}' because you are trying to change its connector type from '{oldConfig.AiConnectorType}' to '{Configuration.AiConnectorType}'. " +
                        $"Changing the AI connector type requires recreating the embeddings to maintain data consistency. " +
                        $"To proceed with these changes:{Environment.NewLine}" +
                        $"1. Delete the existing ETL task{Environment.NewLine}" +
                        $"2. Create a new ETL task with your desired connector type{Environment.NewLine}" +
                        "This will ensure all documents are processed with consistent settings and maintain data integrity.");
                }

                var differences = oldConfig.Connection.Compare(Configuration.Connection);
                if (differences.HasFlag(AiSettingsCompareDifferences.RequiresEmbeddingsRegeneration))
                {
                    throw new RachisApplyException(
                        $"Cannot update AI ETL task '{Configuration.Name}' because it contains critical changes in the connection settings that would affect the structure or creation process of embeddings. " +
                        $"Changes to parameters like model selection, tokenization settings, embedding dimensions, or normalization options require recreating all embeddings to maintain consistency. " +
                        $"To proceed with these changes:{Environment.NewLine}" +
                        $"1. Delete the existing ETL task{Environment.NewLine}" +
                        $"2. Create a new ETL task with your desired settings{Environment.NewLine}" +
                        "This will ensure all documents are processed with consistent settings and maintain data integrity. " +
                        "Note: While you can update non-critical settings like API keys or endpoints without recreating the task, your current changes include critical modifications that affect the embedding process.");
                }
            }
            catch (Exception e) when (ClusterStateMachine.ExpectedException(e))
            {
                throw;
            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to validate AI ETL configuration", e);
            }
        }
    }
}
