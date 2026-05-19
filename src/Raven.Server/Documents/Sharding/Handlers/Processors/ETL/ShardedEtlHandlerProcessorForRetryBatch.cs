using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.ETL;

internal sealed class ShardedEtlHandlerProcessorForRetryBatch : AbstractEtlHandlerProcessorForRetryBatch<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedEtlHandlerProcessorForRetryBatch([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }
    
    protected override bool SupportsCurrentNode => false;
    
    protected override ValueTask HandleCurrentNodeAsync() => throw new NotSupportedException();
    
    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token)
    {
        var shardNumber = GetShardNumber();
    
        return RequestHandler.DatabaseContext.ShardExecutor.ExecuteSingleShardAsync(command, shardNumber, token.Token);
    }
}
