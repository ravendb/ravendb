using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Json;
using Sparrow.Json;

namespace Raven.Server.Web.Operations.Processors;

internal abstract class AbstractOperationsHandlerProcessorForGetNextOperationId<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOperationsHandlerProcessorForGetNextOperationId([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract long GetNextOperationId();

    public override async ValueTask ExecuteAsync()
    {
        var nextId = GetNextOperationId();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                writer.WriteNextOperationIdAndNodeTag(nextId, RequestHandler.ServerStore.NodeTag);
            }
        }
    }
}
