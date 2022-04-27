using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;

internal class ShardedHiLoHandlerProcessorForReturnHiLo : AbstractShardedHiLoHandlerProcessor
{
    public ShardedHiLoHandlerProcessorForReturnHiLo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleHiLoAsync(string tag)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            await ExecuteShardedHiLoCommandAsync(context, tag);
        }

        RequestHandler.NoContentStatus();
    }
}
