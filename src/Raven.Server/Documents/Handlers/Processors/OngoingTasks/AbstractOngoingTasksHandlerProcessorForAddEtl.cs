using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
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

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var id = RequestHandler.GetLongQueryString("id", required: false);

            if (id == null)
            {
                return RequestHandler.ServerStore.AddEtl(context, RequestHandler.DatabaseName, configuration, raftRequestId);
            }

            return RequestHandler.ServerStore.UpdateEtl(context, RequestHandler.DatabaseName, id.Value, configuration, raftRequestId);
        }

        protected  virtual void AssertCanAddOrUpdateEtl(ref BlittableJsonReaderObject etlConfiguration)
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
                default:
                    throw new NotSupportedException($"Unknown ETL configuration type. Configuration: {etlConfiguration}");
            }
        }
    }
}
