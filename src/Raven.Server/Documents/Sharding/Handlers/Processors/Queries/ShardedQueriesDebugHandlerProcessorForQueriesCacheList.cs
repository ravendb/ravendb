using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Debugging.Processors;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Queries;

internal sealed class ShardedQueriesDebugHandlerProcessorForQueriesCacheList : AbstractQueriesDebugHandlerProcessorForQueriesCacheList<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedQueriesDebugHandlerProcessorForQueriesCacheList([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override QueryMetadataCache GetQueryMetadataCache() => RequestHandler.DatabaseContext.QueryMetadataCache;
}
