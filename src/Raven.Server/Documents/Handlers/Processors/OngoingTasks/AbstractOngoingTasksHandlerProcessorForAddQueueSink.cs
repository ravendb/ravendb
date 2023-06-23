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
    internal abstract class AbstractOngoingTasksHandlerProcessorForAddQueueSink<TRequestHandler, TOperationContext> :
        AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler,
            TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _taskId;

        protected AbstractOngoingTasksHandlerProcessorForAddQueueSink([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson,
            BlittableJsonReaderObject configuration, long index)
        {
            _taskId = index;

            responseJson[nameof(EtlConfiguration<ConnectionString>.TaskId)] = _taskId;
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration,
            JsonOperationContext context)
        {
            AssertCanAddOrUpdateQueueSink(ref configuration);
        }

        protected override async ValueTask OnAfterUpdateConfiguration(TransactionOperationContext _,
            BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.AddQueueSinkDebugTag, _taskId, configuration);
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context,
            BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var id = RequestHandler.GetLongQueryString("id", required: false);

            if (id == null)
            {
                return RequestHandler.ServerStore.AddQueueSink(context, RequestHandler.DatabaseName, configuration,
                    raftRequestId);
            }

            return RequestHandler.ServerStore.UpdateQueueSink(context, RequestHandler.DatabaseName, id.Value,
                configuration,
                raftRequestId);
        }

        protected virtual void AssertCanAddOrUpdateQueueSink(ref BlittableJsonReaderObject etlConfiguration)
        {
            RequestHandler.ServerStore.LicenseManager.AssertCanAddQueueSink();
        }
    }
}
