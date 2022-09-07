using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Commands;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers;
using Raven.Server.Documents.Handlers.Processors.HiLo;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.HiLo;

internal class ShardedHiLoHandlerProcessorForReturnHiLo : AbstractHiLoHandlerProcessorForReturnHiLo<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedHiLoHandlerProcessorForReturnHiLo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var tag = GetTag();
        var hiloDocId = HiLoHandler.RavenHiloIdPrefix + tag;

        int shardNumber;
        using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, hiloDocId);

        var command = CreateCommand();
        var proxyCommand = new ProxyCommand(command, HttpContext.Response);

        await RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, shardNumber);
    }

    private RavenCommand CreateCommand()
    {
        var tag = GetTag();
        var last = GetLast();
        var end = GetEnd();

        return new HiLoReturnCommand(tag, last, end);
    }
}
