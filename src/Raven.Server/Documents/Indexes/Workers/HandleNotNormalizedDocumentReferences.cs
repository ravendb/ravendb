using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config.Categories;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Voron;

namespace Raven.Server.Documents.Indexes.Workers;

public class HandleNotNormalizedDocumentReferences : HandleDocumentReferences
{
    public HandleNotNormalizedDocumentReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexingConfiguration configuration) : base(index, referencedCollections, documentsStorage, indexStorage, configuration)
    {
    }

    protected HandleNotNormalizedDocumentReferences(Index index, Dictionary<string, HashSet<CollectionName>> referencedCollections, DocumentsStorage documentsStorage, IndexStorage indexStorage, IndexStorage.ReferencesBase referencesStorage, IndexingConfiguration configuration) : base(index, referencedCollections, documentsStorage, indexStorage, referencesStorage, configuration)
    {
    }

    protected override IndexItem GetItem(DocumentsOperationContext databaseContext, Slice key)
    {
        return GetNonNormalizedDocumentItem(databaseContext, key);
    }

    public static unsafe IndexItem GetNonNormalizedDocumentItem(DocumentsOperationContext databaseContext, Slice key)
    {
        using (DocumentIdWorker.GetLower(databaseContext.Allocator, key.Content.Ptr, key.Size, out var loweredKey))
        {
            var documentItem = GetDocumentItem(databaseContext, loweredKey);
            if (documentItem == null)
            {
                // this isn't required for new indexes as CleanupDocuments will handle it.
                // however, for older indexes, we need to clean up any leftovers.
                CurrentIndexingScope.Current.ReferencesToDelete ??= new HashSet<Slice>();
                CurrentIndexingScope.Current.ReferencesToDelete.Add(key.Clone(databaseContext.Allocator));
            }

            return documentItem;
        }
    }

    protected override void AfterGetItemsFromCollectionThatReference(string collection, IndexingStatsScope stats, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
    {
        AfterGetItemsFromCollectionThatReference(collection, stats, _referencesStorage, databaseContext, indexContext);
    }

    public static void AfterGetItemsFromCollectionThatReference(string collection, IndexingStatsScope stats, IndexStorage.ReferencesBase referencesStorage, DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
    {
        if (CurrentIndexingScope.Current.ReferencesToDelete == null)
            return;

        using (stats.For(IndexingOperation.Map.DocumentRead, start: false).For(IndexingOperation.Storage.UpdateReferences))
        {
            foreach (var keyToRemove in CurrentIndexingScope.Current.ReferencesToDelete)
            {
                referencesStorage.RemoveReferences(keyToRemove, collection, null, indexContext.Transaction);
                keyToRemove.Release(databaseContext.Allocator);
            }
        }

        CurrentIndexingScope.Current.ReferencesToDelete.Clear();
    }
}
