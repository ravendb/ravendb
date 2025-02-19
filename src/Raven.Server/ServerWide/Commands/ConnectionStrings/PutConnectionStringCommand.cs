using System;
using System.Linq;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.OngoingTasks;
using Raven.Server.Rachis;
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

    public sealed class PutAiConnectionStringCommand : PutConnectionStringCommand<AiConnectionString>
    {
        public PutAiConnectionStringCommand()
        {
            // for deserialization
        }

        public PutAiConnectionStringCommand(AiConnectionString connectionString, string databaseName, string uniqueRequestId) : base(connectionString, databaseName, uniqueRequestId)
        {

        }

        public override void UpdateDatabaseRecord(DatabaseRecord record, long etag)
        {
            try
            {
                ConnectionString.Identifier ??= ConnectionString.GenerateIdentifier();
            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to generate AI connection string identifier", e);
            }

            InClusterValidation(record);

            record.AiConnectionStrings[ConnectionString.Name] = ConnectionString;
        }

        private void InClusterValidation(DatabaseRecord databaseRecord)
        {
            try
            {
                if (databaseRecord == null)
                    throw new RachisApplyException("Failed to get database record, but it is required for further validation");

                if (string.IsNullOrWhiteSpace(ConnectionString.Identifier))
                    throw new RachisApplyException("Connection string identifier must be set, but it is not");

                if (ConnectionString.ValidateIdentifier(out var errors) == false)
                    throw new RachisApplyException( $"Invalid identifier format. Validation errors:{Environment.NewLine} - {string.Join($"{Environment.NewLine} - ", errors)}");

                var isUpdate = databaseRecord.AiConnectionStrings.TryGetValue(ConnectionString.Name, out var oldAiConnectionString) && oldAiConnectionString != null;
                var identifierConflicts = databaseRecord?.AiConnectionStrings
                    .Where(x => x.Value != null && x.Value.Identifier == ConnectionString.Identifier && x.Key != ConnectionString.Name)
                    .ToArray();

                if (identifierConflicts.Length > 0)
                    throw new RachisApplyException(
                        $"Can't {(isUpdate ? "update" : "create")} connection string: '{ConnectionString.Name}'. " +
                        $"The identifier '{ConnectionString.Identifier}' is already used by " +
                        $"connection string{(identifierConflicts.Length > 1 ? "s" : "")} " +
                        $"'{string.Join("', '", identifierConflicts.Select(x => x.Key))}'");

                var etlsUsingConnection = databaseRecord.AiIntegrations.Where(x => x.ConnectionStringName == ConnectionString.Name).ToArray();
                var isConnectionStringInUse = etlsUsingConnection.Length > 0;

                if (isUpdate == false || isConnectionStringInUse == false)
                    return;

                var differences = ConnectionString.Compare(oldAiConnectionString);
                if (differences.HasFlag(AiSettingsCompareDifferences.RequiresEmbeddingsRegeneration) == false)
                    return;

                var etlNames = string.Join("', '", etlsUsingConnection.Select(x => x.Name));
                throw new RachisApplyException(
                    $"Cannot update connection string '{ConnectionString.Name}' because it contains changes that would affect the structure or creation process of embeddings. " +
                    $"Changes to parameters like model selection, tokenization settings, embedding dimensions, or normalization options require recreating all embeddings to maintain consistency. " +
                    $"To proceed with these changes:{Environment.NewLine}" +
                    $"1. Delete the existing AI Integration task{(etlsUsingConnection.Length == 1 ? "" : "s")}{Environment.NewLine}" +
                    $"2. {(etlsUsingConnection.Length == 1 ?
                        "After deleting the AI Integration task, you can either update this connection string or create a new one with your desired settings" :
                        $"Create a new connection string with your desired settings, as this connection string is used by AI Integration tasks: '{etlNames}'")}{Environment.NewLine}" +
                    $"3. Create a new AI Integration task using the {(etlsUsingConnection.Length == 1 ? "updated or new" : "new")} connection string{Environment.NewLine}" +
                    "This will ensure all documents are processed with consistent settings and maintain data integrity. " +
                    "Note: While you can update non-critical settings like API keys or endpoints without recreating the task, your current changes include critical modifications that affect the embedding process.");
            }
            catch (Exception e) when (ClusterStateMachine.ExpectedException(e))
            {
                throw;
            }
            catch (Exception e)
            {
                throw new RachisApplyException("Failed to validate AI connection string", e);
            }
        }
    }
}
