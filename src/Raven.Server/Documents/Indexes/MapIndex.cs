using System.Linq;

using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public class MapIndex : MapIndex<IndexDefinitionBase>
    {
        protected MapIndex(int indexId, IndexType type)
            : base(indexId, type, null)
        {
        }
    }

    public abstract class MapIndex<TIndexDefinition> : Index<TIndexDefinition>
        where TIndexDefinition : IndexDefinitionBase
    {
        protected MapIndex(int indexId, IndexType type, TIndexDefinition definition)
            : base(indexId, type, definition)
        {
        }

        protected override bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext)
        {
            using (databaseContext.OpenReadTransaction())
            using (indexContext.OpenReadTransaction())
            {
                foreach (var collection in Collections)
                {
                    var lastCollectionEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, collection);
                    var lastProcessedCollectionEtag = ReadLastMappedEtag(indexContext.Transaction, collection);

                    if (lastCollectionEtag > lastProcessedCollectionEtag)
                        return true;

                    var lastCollectionTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(indexContext, collection);
                    var lastProcessedCollectionTombstoneEtag = ReadLastTombstoneEtag(indexContext.Transaction, collection);

                    if (lastCollectionTombstoneEtag > lastProcessedCollectionTombstoneEtag)
                        return true;
                }

                return false;
            }
        }
    }
}