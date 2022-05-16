using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Operations.Processors;

internal abstract class AbstractOperationsHandlerProcessorForKill<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOperationsHandlerProcessorForKill([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected abstract ValueTask KillOperationAsync(long operationId, CancellationToken token);

    public override async ValueTask ExecuteAsync()
    {
        var id = RequestHandler.GetLongQueryString("id");

        await KillOperationAsync(id, CancellationToken.None);

        RequestHandler.NoContentStatus();
    }
}
