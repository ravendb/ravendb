using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Web.Operations.Processors;

internal class OperationsHandlerProcessorForKill : AbstractOperationsHandlerProcessorForKill<DatabaseRequestHandler, DocumentsOperationContext>
{
    public OperationsHandlerProcessorForKill([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask KillOperationAsync(long operationId, CancellationToken token) => RequestHandler.Database.Operations.KillOperationAsync(operationId, token);
}
