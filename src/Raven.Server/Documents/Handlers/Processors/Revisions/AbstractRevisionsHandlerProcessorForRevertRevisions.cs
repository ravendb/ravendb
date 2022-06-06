using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Revisions;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Revisions
{
    internal abstract class AbstractRevisionsHandlerProcessorForRevertRevisions<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
        where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
        where TOperationContext : JsonOperationContext
    {
        protected AbstractRevisionsHandlerProcessorForRevertRevisions([NotNull] TRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected abstract void ScheduleRevertRevisions(long operationId, RevertRevisionsRequest configuration, OperationCancelToken token);

        protected abstract long GetNextOperationId();

        public override async ValueTask ExecuteAsync()
        {
            RevertRevisionsRequest configuration;

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "revisions/revert");

                configuration = JsonDeserializationServer.RevertRevisions(json);
            }

            var token = RequestHandler.CreateTimeLimitedOperationToken();
            var operationId = RequestHandler.GetLongQueryString("operationId", required: false) ?? GetNextOperationId();

            ScheduleRevertRevisions(operationId, configuration, token);

            using (ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteOperationIdAndNodeTag(context, operationId, ServerStore.NodeTag);
            }
        }
    }
}
