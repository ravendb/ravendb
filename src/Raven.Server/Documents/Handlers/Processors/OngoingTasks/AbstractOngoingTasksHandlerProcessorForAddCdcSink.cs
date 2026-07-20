using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.CdcSink;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.OngoingTasks
{
    internal abstract class AbstractOngoingTasksHandlerProcessorForAddCdcSink<TRequestHandler, TOperationContext> :
        AbstractHandlerProcessorForUpdateDatabaseConfiguration<BlittableJsonReaderObject, TRequestHandler,
            TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    {
        private long _taskId;

        protected AbstractOngoingTasksHandlerProcessorForAddCdcSink([NotNull] TRequestHandler requestHandler)
            : base(requestHandler)
        {
        }

        protected override void OnBeforeResponseWrite(TransactionOperationContext _, DynamicJsonValue responseJson,
            BlittableJsonReaderObject configuration, long index)
        {
            _taskId = index;

            responseJson[nameof(CdcSinkConfiguration.TaskId)] = _taskId;
        }

        protected override void OnBeforeUpdateConfiguration(ref BlittableJsonReaderObject configuration,
            JsonOperationContext context)
        {
            AssertCanAddOrUpdateCdcSink(ref configuration);
        }

        protected override ValueTask OnAfterUpdateConfiguration(TransactionOperationContext _,
            BlittableJsonReaderObject configuration, string raftRequestId)
        {
            RequestHandler.LogTaskToAudit(Web.RequestHandler.AddCdcSinkDebugTag, _taskId, configuration);

            return ValueTask.CompletedTask;
        }

        protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context,
            BlittableJsonReaderObject configuration, string raftRequestId)
        {
            var id = RequestHandler.GetLongQueryString("id", required: false);

            if (id == null)
            {
                return RequestHandler.ServerStore.AddCdcSink(context, RequestHandler.DatabaseName, configuration,
                    raftRequestId);
            }

            return RequestHandler.ServerStore.UpdateCdcSink(context, RequestHandler.DatabaseName, id.Value,
                configuration,
                raftRequestId);
        }

        protected virtual void AssertCanAddOrUpdateCdcSink(ref BlittableJsonReaderObject cdcSinkConfiguration)
        {
        }
    }
}
