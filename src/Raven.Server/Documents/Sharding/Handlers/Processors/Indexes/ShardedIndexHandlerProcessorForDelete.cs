using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForDelete : AbstractIndexHandlerProcessorForDelete<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForDelete([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override AbstractIndexDeleteProcessor GetIndexDeleteProcessor() => RequestHandler.DatabaseContext.Indexes.Delete;

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
}
