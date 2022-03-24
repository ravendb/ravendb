using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal abstract class AbstractIndexHandlerProcessorForSetPriority<TRequestHandler, TOperationContext> : AbstractHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractIndexHandlerProcessorForSetPriority([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract AbstractIndexPriorityController GetIndexPriorityProcessor();

    public override async ValueTask ExecuteAsync()
    {
        var raftRequestId = RequestHandler.GetRaftRequestIdFromQuery();
        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "index/set-priority");
            var parameters = JsonDeserializationServer.Parameters.SetIndexPriorityParameters(json);

            var processor = GetIndexPriorityProcessor();

            for (var index = 0; index < parameters.IndexNames.Length; index++)
            {
                var indexName = parameters.IndexNames[index];
                await processor.SetPriorityAsync(indexName, parameters.Priority, $"{raftRequestId}/{index}");
            }
        }

        RequestHandler.NoContentStatus();
    }
}
