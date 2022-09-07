using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
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
    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        var tag = GetTag();
        var hiloDocId = HiLoHandler.RavenHiloIdPrefix + tag;

        int shardNumber;
        using (RequestHandler.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, hiloDocId);

        return RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
