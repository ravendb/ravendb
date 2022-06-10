using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal class ShardedEtlHandlerProcessorForDebugStats : AbstractEtlHandlerProcessorForDebugStats<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedEtlHandlerProcessorForDebugStats([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task HandleRemoteNodeAsync(ProxyCommand<EtlTaskDebugStats[]> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();

        return RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
