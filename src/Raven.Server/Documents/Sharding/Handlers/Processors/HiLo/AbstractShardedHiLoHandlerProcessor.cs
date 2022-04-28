using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;

internal abstract class AbstractShardedHiLoHandlerProcessor : AbstractHiLoHandlerProcessor<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    protected AbstractShardedHiLoHandlerProcessor([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected async ValueTask<ShardedCommand> ExecuteShardedHiLoCommandAsync(TransactionOperationContext context, string tag)
    {
        var hiloDocId = HiLoHandler.RavenHiloIdPrefix + tag;
        var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, hiloDocId);

        var cmd = new ShardedCommand(RequestHandler, Headers.None);
        await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber);

        HttpContext.Response.StatusCode = (int)cmd.StatusCode;
        HttpContext.Response.Headers[Constants.Headers.Etag] = cmd.Response?.Headers?.ETag?.Tag;

        return cmd;
    }
}
