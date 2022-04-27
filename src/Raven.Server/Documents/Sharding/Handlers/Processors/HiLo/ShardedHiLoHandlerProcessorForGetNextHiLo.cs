using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;

internal class ShardedHiLoHandlerProcessorForGetNextHiLo : AbstractHiLoHandlerProcessorForGetNextHiLo<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedHiLoHandlerProcessorForGetNextHiLo([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override async ValueTask HandleGetNextHiLoAsync(string tag)
    {
        using (ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var cmd = await ExecuteShardedHiloCommandAsync(context, RequestHandler, tag);
            await cmd.Result.WriteJsonToAsync(RequestHandler.ResponseBodyStream());
        }
    }

    private static async ValueTask<ShardedCommand> ExecuteShardedHiloCommandAsync(TransactionOperationContext context, ShardedDatabaseRequestHandler requestHandler, string tag)
    {
        var hiloDocId = HiLoHandler.RavenHiloIdPrefix + tag;
        var shardNumber = requestHandler.DatabaseContext.GetShardNumber(context, hiloDocId);

        var cmd = new ShardedCommand(requestHandler, Headers.None);
        await requestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber);

        requestHandler.HttpContext.Response.StatusCode = (int)cmd.StatusCode;
        requestHandler.HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

        return cmd;
    }
}
