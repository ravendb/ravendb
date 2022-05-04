using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForSuggestIndexMerge : AbstractIndexHandlerProcessorForSuggestIndexMerge<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForSuggestIndexMerge([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override Dictionary<string, IndexDefinition> GetIndexes()
    {
        var indexes = new Dictionary<string, IndexDefinition>();
        foreach (Index index in RequestHandler.Database.IndexStore.GetIndexes())
            indexes[index.Name] = index.GetIndexDefinition();

        return indexes;
    }
}
