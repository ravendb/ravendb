using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForHasChanged : AbstractIndexHandlerProcessorForHasChanged<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForHasChanged([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override AbstractIndexHasChangedController GetHasChangedController() => RequestHandler.DatabaseContext.Indexes.HasChanged;
}
