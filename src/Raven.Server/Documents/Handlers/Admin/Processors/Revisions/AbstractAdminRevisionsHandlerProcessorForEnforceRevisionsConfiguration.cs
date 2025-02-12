using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Operations;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Admin.Processors.Revisions
{
    internal abstract class AbstractAdminRevisionsHandlerProcessorForRevisionsOperation<TRequestHandler, TOperationContext, TOperationParameters> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TOperationContext : JsonOperationContext
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationParameters : IRevisionsOperationParameters
    {
        protected OperationType _operationType;
        public abstract string Description { get; }

        protected AbstractAdminRevisionsHandlerProcessorForRevisionsOperation([NotNull] TRequestHandler requestHandler, OperationType operationType) : base(requestHandler)
        {
            _operationType = operationType;
        }

        protected abstract long GetNextOperationId();

        protected abstract void ScheduleEnforceConfigurationOperation(long operationId, TOperationParameters parameters, OperationCancelToken token);
        protected abstract TOperationParameters GetOperationParameters(BlittableJsonReaderObject json);


        public override async ValueTask ExecuteAsync()
        {
            var token = RequestHandler.CreateBackgroundOperationToken();
            var operationId = RequestHandler.GetLongQueryString("operationId", false) ?? GetNextOperationId();

            TOperationParameters parameters;
            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/revert");
                parameters = GetOperationParameters(json);
            }

            ScheduleEnforceConfigurationOperation(operationId, parameters, token);

            using (ClusterContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }
}
