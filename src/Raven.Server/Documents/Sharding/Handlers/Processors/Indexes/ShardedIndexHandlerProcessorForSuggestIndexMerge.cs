using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Handlers.Processors.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Indexes;

internal class ShardedIndexHandlerProcessorForSuggestIndexMerge : AbstractIndexHandlerProcessorForSuggestIndexMerge<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedIndexHandlerProcessorForSuggestIndexMerge([NotNull] ShardedDatabaseRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    protected override Dictionary<string, IndexDefinition> GetIndexes()
    {
        var indexes = new Dictionary<string, IndexDefinition>();

        foreach (var index in RequestHandler.DatabaseContext.Indexes.GetIndexes())
            indexes[index.Name] = index.Definition.GetOrCreateIndexDefinitionInternal();

        return indexes;
    }
}
