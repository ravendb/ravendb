using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Admin.Processors.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForSource : AbstractIndexHandlerProcessorForSource<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForSource([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler, requestHandler.ContextPool)
    {
    }

    protected override IndexInformationHolder GetIndex(string name) => RequestHandler.DatabaseContext.Indexes.GetIndex(name);
}
