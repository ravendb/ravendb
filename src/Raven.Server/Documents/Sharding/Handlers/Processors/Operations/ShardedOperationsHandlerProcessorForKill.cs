using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Operations.Processors;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Operations;

internal class ShardedOperationsHandlerProcessorForKill : AbstractOperationsHandlerProcessorForKill<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOperationsHandlerProcessorForKill([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override ValueTask KillOperationAsync(long operationId, CancellationToken token) => RequestHandler.DatabaseContext.Operations.KillOperationAsync(operationId, token);
}
