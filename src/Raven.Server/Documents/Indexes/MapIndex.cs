using System;
using Raven.Server.Documents.Indexes.Persistance.Lucene;
using Raven.Server.Json;

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

        protected override bool IsStale(RavenOperationContext databaseContext, RavenOperationContext indexContext, out long lastEtag)
        {
            long lastDocumentEtag;
            using (var tx = databaseContext.OpenReadTransaction())
            {
                lastDocumentEtag = DocumentsStorage.ReadLastEtag(tx.InnerTransaction);
            }

            long lastMappedEtag;
            using (var tx = indexContext.OpenReadTransaction())
            {
                lastMappedEtag = ReadLastMappedEtag(tx);
            }

            lastEtag = lastMappedEtag;
            return lastDocumentEtag > lastMappedEtag;
        }
    }
}