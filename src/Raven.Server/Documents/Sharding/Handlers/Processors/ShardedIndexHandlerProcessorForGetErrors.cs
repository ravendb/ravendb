using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors;

internal class ShardedIndexHandlerProcessorForGetErrors : AbstractIndexHandlerProcessorForGetErrors<ShardedRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForGetErrors([NotNull] ShardedRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override bool SupportsCurrentNode => false;

    protected override ValueTask<IndexErrors[]> GetResultForCurrentNodeAsync() => throw new NotSupportedException();

    protected override Task<IndexErrors[]> GetResultForRemoteNodeAsync(RavenCommand<IndexErrors[]> command) => RequestHandler.ShardExecutor.ExecuteSingleShardAsync(command, GetShardNumber());
}
