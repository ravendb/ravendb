using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.Sorters;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Sorters;

internal class ShardedSortersHandlerProcessorForGet : AbstractSortersHandlerProcessorForGet<ShardedDatabaseRequestHandler>
{
    public ShardedSortersHandlerProcessorForGet([NotNull] ShardedDatabaseRequestHandler requestHandler) 
        : base(requestHandler)
    {
    }

    protected override string GetDatabaseName() => RequestHandler.DatabaseContext.DatabaseName;
}
