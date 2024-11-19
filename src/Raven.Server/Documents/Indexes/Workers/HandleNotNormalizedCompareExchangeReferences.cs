using System.Collections.Generic;
using Raven.Server.Config.Categories;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers;

public class HandleNotNormalizedCompareExchangeReferences : HandleCompareExchangeReferences
{
    public HandleNotNormalizedCompareExchangeReferences(Index index, HashSet<string> collectionsWithCompareExchangeReferences, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration) : base(index, collectionsWithCompareExchangeReferences, documentsStorage, indexStorage, configuration)
    {
    }

    protected override IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
    {
        return HandleNotNormalizedDocumentReferences.GetNonNormalizedDocumentItem(databaseContext, key);
    }

    protected override void DeleteReferences(string collection, IndexingStatsScope stats, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
    {
        HandleNotNormalizedDocumentReferences.DeleteReferences(collection, stats, _referencesStorage, databaseContext, indexContext);
    }
}
