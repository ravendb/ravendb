using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;

internal class ShardedHiLoHandlerProcessorForGetNextHiLo : AbstractShardedHiLoHandlerProcessor
{
    public ShardedHiLoHandlerProcessorForGetNextHiLo([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override async ValueTask HandleHiLoAsync(string tag)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var cmd = await ExecuteShardedHiLoCommandAsync(context, tag);
            await cmd.Result.WriteJsonToAsync(RequestHandler.ResponseBodyStream());
        }
    }
}
