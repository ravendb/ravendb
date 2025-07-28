using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForAddEtl<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _taskId;
        protected AbstractOngoingTasksHandlerProcessorForAddEtl([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson, BlittableJsonReaderObject configuration, long index)
        {
            _taskId = index;

            responseJson[nameof(EtlConfiguration<ConnectionString>.TaskId)] = _taskId;
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration, JsonOperationContext context)
        {
            AssertCanAddOrUpdateEtl(ref configuration);

            string identifierProp = null;
            string debugTag = null;

            switch (EtlConfiguration<ConnectionString>.GetEtlType(configuration))
            {
                case EtlType.EmbeddingsGeneration:
                {
                    identifierProp = nameof(EmbeddingsGenerationConfiguration.Identifier);
                    debugTag = "EmbeddingsGenerationConfig";
                    break;
                }
                
                case EtlType.GenAi:
                {
                    identifierProp = nameof(GenAiConfiguration.Identifier);
                    debugTag = "GenAiConfig";
                    break;
                }

                default:
                {
                    return;
                }
            }

            if (configuration.TryGet(identifierProp, out string id) == false || string.IsNullOrEmpty(id))
            {
                configuration.TryGet(nameof(AbstractAiIntegrationConfiguration.Name), out string name);
                var identifier = AiTaskIdentifierHelper.GenerateIdentifier(name);

                configuration.Modifications = new DynamicJsonValue(configuration)
                {
                    [identifierProp] = identifier
                };
            }

            if (configuration.Modifications !=  null)
            {
                configuration = context.ReadObject(configuration, debugTag);
            }
        }

        protected override async ValueTask OnAfterUpdateConfiguration(TransactionOperationContext _, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.AddEtlDebugTag, _taskId, configuration);

            // Reset scripts if needed
            var scriptsToReset = RequestHandler.GetStringValuesQueryString("reset", required: false);
            configuration.TryGet(nameof(RavenEtlConfiguration.Name), out string etlConfigurationName);

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
            using (ctx.OpenReadTransaction())
            {
                foreach (var script in scriptsToReset)
                {
                    await RequestHandler.ServerStore.RemoveEtlProcessState(ctx, RequestHandler.DatabaseName, etlConfigurationName, script, $"{raftRequestId}/{script}");
                }
            }
        }

        protected virtual string GetChangeVector() => RequestHandler.GetStringQueryString("changeVector", required: false);

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var id = RequestHandler.GetLongQueryString("id", required: false);
            var changeVector = GetChangeVector();

            if (id == null)
            {
                return RequestHandler.ServerStore.AddEtl(context, RequestHandler.DatabaseName, configuration, changeVector, raftRequestId);
            }

            return RequestHandler.ServerStore.UpdateEtl(context, RequestHandler.DatabaseName, id.Value, configuration, changeVector, raftRequestId);
        }

        protected virtual void AssertCanAddOrUpdateEtl(ref BlittableJsonReaderObject etlConfiguration)
        {
            switch (EtlConfiguration<ConnectionString>.GetEtlType(etlConfiguration))
            {
                case EtlType.Raven:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddRavenEtl();
                    break;
                case EtlType.Sql:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddSqlEtl();
                    break;
                case EtlType.Olap:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddOlapEtl();
                    break;
                case EtlType.ElasticSearch:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddElasticSearchEtl();
                    break;
                case EtlType.Queue:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddQueueEtl();
                    break;
                case EtlType.Snowflake:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddSnowflakeEtl();
                    break;
                case EtlType.EmbeddingsGeneration:
                    using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                    using (context.OpenReadTransaction())
                    {
                        var embeddingsGenerationConfiguration = Client.Json.Serialization.JsonDeserializationClient.EmbeddingsGenerationConfiguration(etlConfiguration);
                        var connectionStringName = embeddingsGenerationConfiguration.ConnectionStringName ?? string.Empty;
                        RequestHandler.ServerStore.LicenseManager.AssertCanAddEmbeddingsGenerationTask(GetAiConnectionString(context, connectionStringName));
                        break;
                    }
                case EtlType.GenAi:
                    RequestHandler.ServerStore.LicenseManager.AssertCanAddGenAiTask();
                    break;

                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }

            AiConnectionString GetAiConnectionString(TransactionOperationContext context, string connectionStringName)
            {
                var database = RequestHandler.ServerStore.Cluster.ReadRawDatabaseRecord(context, RequestHandler.DatabaseName);
                AiConnectionString aiConnectionString = null;
                database?.AiConnectionStrings?.TryGetValue(connectionStringName, out aiConnectionString);
                return aiConnectionString;
            }
        }
    }
}
